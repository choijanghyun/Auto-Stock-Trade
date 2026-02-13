"""
KATS RealtimeCache - In-Memory Price/Orderbook/VI Cache

WebSocket callbacks WRITE data into the cache.
Strategy engine and risk manager READ ONLY from the cache.
No strategy module should ever call REST API for realtime quotes.

Thread safety: asyncio.Lock guards all write operations.
Read operations are lock-free for minimal latency.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


# ── Dataclasses ──────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class PriceData:
    """Latest trade execution data for a single stock."""

    price: float
    volume: int
    change_pct: float
    timestamp: float  # time.monotonic() at reception


@dataclass(frozen=True, slots=True)
class OrderbookData:
    """10-level orderbook snapshot."""

    ask_prices: List[float]    # ask_prices[0] = best ask (lowest)
    ask_volumes: List[int]     # corresponding ask volumes
    bid_prices: List[float]    # bid_prices[0] = best bid (highest)
    bid_volumes: List[int]     # corresponding bid volumes
    total_ask_volume: int
    total_bid_volume: int
    timestamp: float


@dataclass(frozen=True, slots=True)
class VIStatus:
    """Volatility Interruption (VI) status for a stock."""

    state: str              # "NORMAL", "TRIGGERED", "COOLING"
    reference_price: float  # VI reference price (previous close)
    static_upper: float     # static VI upper bound (ref * 1.10)
    static_lower: float     # static VI lower bound (ref * 0.90)
    triggered_at: Optional[float] = None  # monotonic timestamp when VI triggered


# ── RealtimeCache ────────────────────────────────────────────────────────────


class RealtimeCache:
    """
    In-Memory cache fed by KIS WebSocket callbacks.

    Principles
    ----------
    - WebSocket callbacks (on_price_update, on_orderbook_update, on_vi_update)
      acquire an asyncio.Lock and **write** data.
    - Strategy reads (get_price, get_orderbook, get_vi_status) are **lock-free**
      because Python dict reads are atomic under the GIL and the data objects are
      immutable (frozen dataclasses).  A stale-read of one event cycle is
      acceptable for trading decisions.
    - If price data is older than 5 seconds a warning is logged on read.
    """

    def __init__(self) -> None:
        self._prices: Dict[str, PriceData] = {}
        self._orderbooks: Dict[str, OrderbookData] = {}
        self._vi_status: Dict[str, VIStatus] = {}
        self._last_update: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    # ── WebSocket Write Callbacks ────────────────────────────────────────

    async def on_price_update(self, stock_code: str, data: dict) -> None:
        """
        Called by WebSocket handler on trade execution data (H0STCNT0).

        Parameters
        ----------
        stock_code : str
            6-digit KRX stock code, e.g. "005930".
        data : dict
            Parsed WebSocket payload containing at minimum:
            - stck_prpr  : current price
            - cntg_vol   : execution volume
            - prdy_ctrt  : change percentage vs previous close
        """
        now = time.monotonic()
        price_data = PriceData(
            price=float(data["stck_prpr"]),
            volume=int(data["cntg_vol"]),
            change_pct=float(data["prdy_ctrt"]),
            timestamp=now,
        )
        async with self._lock:
            self._prices[stock_code] = price_data
            self._last_update[stock_code] = now

        logger.debug(
            "price_cache_updated",
            stock_code=stock_code,
            price=price_data.price,
            volume=price_data.volume,
            change_pct=price_data.change_pct,
        )

    async def on_orderbook_update(self, stock_code: str, data: dict) -> None:
        """
        Called by WebSocket handler on orderbook data (H0STASP0).

        Parameters
        ----------
        stock_code : str
            6-digit KRX stock code.
        data : dict
            Parsed WebSocket payload containing:
            - askp1..askp10         : ask prices (1=best)
            - askp_rsqn1..10       : ask volumes
            - bidp1..bidp10        : bid prices (1=best)
            - bidp_rsqn1..10      : bid volumes
            - total_askp_rsqn      : total ask volume
            - total_bidp_rsqn      : total bid volume
        """
        now = time.monotonic()
        orderbook = OrderbookData(
            ask_prices=[float(data.get(f"askp{i}", 0)) for i in range(1, 11)],
            ask_volumes=[int(data.get(f"askp_rsqn{i}", 0)) for i in range(1, 11)],
            bid_prices=[float(data.get(f"bidp{i}", 0)) for i in range(1, 11)],
            bid_volumes=[int(data.get(f"bidp_rsqn{i}", 0)) for i in range(1, 11)],
            total_ask_volume=int(data.get("total_askp_rsqn", 0)),
            total_bid_volume=int(data.get("total_bidp_rsqn", 0)),
            timestamp=now,
        )
        async with self._lock:
            self._orderbooks[stock_code] = orderbook
            self._last_update[stock_code] = now

        logger.debug(
            "orderbook_cache_updated",
            stock_code=stock_code,
            best_ask=orderbook.ask_prices[0] if orderbook.ask_prices else 0,
            best_bid=orderbook.bid_prices[0] if orderbook.bid_prices else 0,
            total_ask_vol=orderbook.total_ask_volume,
            total_bid_vol=orderbook.total_bid_volume,
        )

    async def on_vi_update(self, stock_code: str, data: dict) -> None:
        """
        Called by WebSocket handler on VI information (H0STVI0).

        Parameters
        ----------
        stock_code : str
            6-digit KRX stock code.
        data : dict
            Parsed WebSocket payload containing:
            - vi_cls_code : "1" = triggered, "2" = released
            - vi_stnd_prc : VI reference price (previous close)
        """
        now = time.monotonic()
        vi_cls = data.get("vi_cls_code", "")
        ref_price = float(data.get("vi_stnd_prc", 0))

        if vi_cls == "1":
            state = "TRIGGERED"
            triggered_at = now
        elif vi_cls == "2":
            state = "COOLING"
            triggered_at = self._vi_status.get(stock_code, VIStatus(
                state="NORMAL", reference_price=0,
                static_upper=0, static_lower=0,
            )).triggered_at
        else:
            state = "NORMAL"
            triggered_at = None

        vi_status = VIStatus(
            state=state,
            reference_price=ref_price,
            static_upper=ref_price * 1.10 if ref_price > 0 else 0.0,
            static_lower=ref_price * 0.90 if ref_price > 0 else 0.0,
            triggered_at=triggered_at,
        )

        async with self._lock:
            self._vi_status[stock_code] = vi_status
            self._last_update[stock_code] = now

        logger.info(
            "vi_cache_updated",
            stock_code=stock_code,
            vi_state=state,
            reference_price=ref_price,
            static_upper=vi_status.static_upper,
            static_lower=vi_status.static_lower,
        )

    # ── Strategy Read Methods (lock-free) ────────────────────────────────

    def get_price(self, stock_code: str) -> Optional[PriceData]:
        """
        Return latest cached price for *stock_code*, or ``None`` if never received.

        Logs a warning if data is older than 5 seconds (stale feed).
        """
        data = self._prices.get(stock_code)
        if data is not None:
            age = time.monotonic() - data.timestamp
            if age > 5.0:
                logger.warning(
                    "stale_price_data",
                    stock_code=stock_code,
                    age_seconds=round(age, 2),
                    msg=f"{stock_code} price data is {age:.1f}s old -- feed may be delayed",
                )
        return data

    def get_orderbook(self, stock_code: str) -> Optional[OrderbookData]:
        """Return latest cached orderbook for *stock_code*, or ``None``."""
        data = self._orderbooks.get(stock_code)
        if data is not None:
            age = time.monotonic() - data.timestamp
            if age > 5.0:
                logger.warning(
                    "stale_orderbook_data",
                    stock_code=stock_code,
                    age_seconds=round(age, 2),
                    msg=f"{stock_code} orderbook data is {age:.1f}s old -- feed may be delayed",
                )
        return data

    def get_vi_status(self, stock_code: str) -> Optional[VIStatus]:
        """Return latest cached VI status for *stock_code*, or ``None``."""
        return self._vi_status.get(stock_code)

    def is_data_fresh(self, stock_code: str, max_age_sec: float = 3.0) -> bool:
        """
        Return ``True`` if *stock_code* has been updated within *max_age_sec*.

        Useful for strategies to guard against acting on stale data.
        """
        last = self._last_update.get(stock_code, 0.0)
        return (time.monotonic() - last) <= max_age_sec

    # ── Utility ──────────────────────────────────────────────────────────

    def tracked_stock_codes(self) -> list[str]:
        """Return a list of all stock codes currently tracked in the cache."""
        codes: set[str] = set()
        codes.update(self._prices.keys())
        codes.update(self._orderbooks.keys())
        return sorted(codes)

    def snapshot(self, stock_code: str) -> dict:
        """
        Return a combined snapshot dict for *stock_code*.

        Convenient for logging, journaling, or strategy indicator snapshots.
        """
        price = self._prices.get(stock_code)
        orderbook = self._orderbooks.get(stock_code)
        vi = self._vi_status.get(stock_code)
        return {
            "stock_code": stock_code,
            "price": price.price if price else None,
            "volume": price.volume if price else None,
            "change_pct": price.change_pct if price else None,
            "best_ask": orderbook.ask_prices[0] if orderbook and orderbook.ask_prices else None,
            "best_bid": orderbook.bid_prices[0] if orderbook and orderbook.bid_prices else None,
            "total_ask_volume": orderbook.total_ask_volume if orderbook else None,
            "total_bid_volume": orderbook.total_bid_volume if orderbook else None,
            "vi_state": vi.state if vi else "UNKNOWN",
            "data_fresh": self.is_data_fresh(stock_code),
        }

    def clear(self) -> None:
        """Clear all cached data (e.g. at end-of-day)."""
        self._prices.clear()
        self._orderbooks.clear()
        self._vi_status.clear()
        self._last_update.clear()
        logger.info("realtime_cache_cleared")
