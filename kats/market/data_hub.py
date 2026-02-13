"""
KATS MarketDataHub - Central Market Data Orchestrator

Coordinates all market data subsystems:
  - RealtimeCache (in-memory price/orderbook/VI)
  - VIMonitor (VI state machine and order gating)
  - RedisTickBuffer (tick-level persistence for post-market bulk write)

Provides a unified ``get_market_data(stock_code)`` interface that
aggregates realtime cache, pre-computed indicators, and historical
data into a single MarketData object consumed by strategy engines.

Design principle: strategy modules NEVER call REST API directly.
They read exclusively from MarketDataHub.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import structlog

from kats.market.indicator_calculator import IndicatorCalculator
from kats.market.realtime_cache import (
    OrderbookData,
    PriceData,
    RealtimeCache,
    VIStatus,
)
from kats.market.vi_monitor import VIMonitor, VIState

logger = structlog.get_logger(__name__)


# ── MarketData Dataclass ─────────────────────────────────────────────────────


@dataclass
class MarketData:
    """
    Aggregated market data snapshot for a single stock.

    This is the primary data object consumed by strategy engines.
    All fields are populated by ``MarketDataHub.get_market_data()``.
    """

    stock_code: str

    # Current realtime data
    current_price: float = 0.0
    current_volume: int = 0
    change_pct: float = 0.0

    # Orderbook
    orderbook: Optional[OrderbookData] = None

    # Pre-computed indicators (from historical + intraday data)
    indicators: Dict[str, Any] = field(default_factory=dict)

    # Previous day OHLCV
    prev_day: Dict[str, float] = field(default_factory=dict)
    # {"open": ..., "high": ..., "low": ..., "close": ..., "volume": ...}

    # Today's session
    today_open: float = 0.0

    # Minute candles (most recent N, for intraday strategies)
    minute_candles: List[Dict[str, Any]] = field(default_factory=list)

    # Daily candles (historical, chronological, oldest first)
    daily_candles: List[Dict[str, Any]] = field(default_factory=list)

    # VI status
    vi_state: str = "NORMAL"
    vi_tradeable: bool = True

    # Data quality
    data_fresh: bool = False
    price_timestamp: Optional[float] = None


# ── MarketDataHub ────────────────────────────────────────────────────────────


class MarketDataHub:
    """
    Central orchestrator for all market data.

    Parameters
    ----------
    cache : RealtimeCache
        In-memory price/orderbook cache fed by WebSocket.
    vi_monitor : VIMonitor
        VI state tracker.
    redis_tick_buffer : object, optional
        Redis-based tick buffer for persistence (``kats.database.redis_buffer``).
        If ``None`` tick buffering is disabled.
    rest_client : object, optional
        KISRestClient for loading historical data.
    """

    def __init__(
        self,
        cache: RealtimeCache,
        vi_monitor: VIMonitor,
        redis_tick_buffer: Any = None,
        rest_client: Any = None,
    ) -> None:
        self.cache = cache
        self.vi_monitor = vi_monitor
        self.redis_tick_buffer = redis_tick_buffer
        self.rest_client = rest_client

        # Per-stock historical data loaded at 08:30 via REST
        self._historical: Dict[str, List[Dict[str, Any]]] = {}
        # {stock_code: [{"date", "open", "high", "low", "close", "volume"}, ...]}

        # Per-stock pre-computed indicators (refreshed on historical load)
        self._indicators: Dict[str, Dict[str, Any]] = {}

        # Per-stock previous day data
        self._prev_day: Dict[str, Dict[str, float]] = {}

        # Per-stock today's open price
        self._today_open: Dict[str, float] = {}

        # Per-stock minute candles
        self._minute_candles: Dict[str, List[Dict[str, Any]]] = {}

    # ── WebSocket Registration ───────────────────────────────────────────

    def register_websocket_callbacks(self, ws_client: Any) -> None:
        """
        Wire up WebSocket client callbacks to the cache and VI monitor.

        Expected *ws_client* interface::

            ws_client.callbacks = {
                "H0STCNT0": async_fn(stock_code, data),  # trade execution
                "H0STASP0": async_fn(stock_code, data),  # orderbook
                "H0STVI0":  async_fn(stock_code, data),  # VI info
            }

        Parameters
        ----------
        ws_client : KISWebSocketClient or compatible
            Must expose a ``callbacks`` dict.
        """
        ws_client.callbacks["H0STCNT0"] = self._on_execution
        ws_client.callbacks["H0STASP0"] = self._on_orderbook
        ws_client.callbacks["H0STVI0"] = self._on_vi

        logger.info(
            "websocket_callbacks_registered",
            registered_tr_ids=["H0STCNT0", "H0STASP0", "H0STVI0"],
        )

    async def _on_execution(self, stock_code: str, data: dict) -> None:
        """Route trade execution data to cache and optional tick buffer."""
        await self.cache.on_price_update(stock_code, data)

        # Buffer tick to Redis for post-market bulk write
        if self.redis_tick_buffer is not None:
            try:
                await self.redis_tick_buffer.push_tick(stock_code, data)
            except Exception:
                logger.warning(
                    "redis_tick_buffer_push_failed",
                    stock_code=stock_code,
                    exc_info=True,
                )

    async def _on_orderbook(self, stock_code: str, data: dict) -> None:
        """Route orderbook data to cache."""
        await self.cache.on_orderbook_update(stock_code, data)

    async def _on_vi(self, stock_code: str, data: dict) -> None:
        """Route VI data to both VI monitor and cache."""
        await self.vi_monitor.on_vi_data(stock_code, data)

    # ── Historical Data Loading ──────────────────────────────────────────

    async def load_historical_data(self, stock_code: str) -> None:
        """
        Fetch daily candles via REST API and pre-compute indicators.

        Designed to be called once per stock at 08:30 KST before market
        open.  Results are cached in-memory for the rest of the session.

        Populates:
          - ``_historical[stock_code]``
          - ``_indicators[stock_code]``
          - ``_prev_day[stock_code]``
        """
        if self.rest_client is None:
            logger.warning(
                "historical_load_skipped",
                stock_code=stock_code,
                reason="no REST client configured",
            )
            return

        try:
            resp = await self.rest_client.get_daily_price(
                stock_code, period="D", count=250
            )
            raw_candles = resp.get("output2", resp.get("output", []))

            if not raw_candles:
                logger.warning(
                    "historical_load_empty",
                    stock_code=stock_code,
                )
                return

            # KIS returns newest-first; reverse to chronological
            daily_data: List[Dict[str, Any]] = []
            for candle in reversed(raw_candles):
                daily_data.append({
                    "date": candle.get("stck_bsop_date", ""),
                    "open": float(candle.get("stck_oprc", 0)),
                    "high": float(candle.get("stck_hgpr", 0)),
                    "low": float(candle.get("stck_lwpr", 0)),
                    "close": float(candle.get("stck_clpr", 0)),
                    "volume": int(candle.get("acml_vol", 0)),
                })

            self._historical[stock_code] = daily_data

            # Pre-compute indicators
            self._indicators[stock_code] = IndicatorCalculator.calculate_all(daily_data)

            # Previous day data (last candle)
            if daily_data:
                last = daily_data[-1]
                self._prev_day[stock_code] = {
                    "open": last["open"],
                    "high": last["high"],
                    "low": last["low"],
                    "close": last["close"],
                    "volume": float(last["volume"]),
                }

                # Initialize VI prices from previous close
                self.vi_monitor.initialize_vi_prices(stock_code, last["close"])

            logger.info(
                "historical_data_loaded",
                stock_code=stock_code,
                candle_count=len(daily_data),
                indicator_keys=list(self._indicators[stock_code].keys()),
            )

        except Exception:
            logger.exception(
                "historical_data_load_failed",
                stock_code=stock_code,
            )

    async def load_historical_batch(self, stock_codes: List[str]) -> None:
        """Load historical data for multiple stocks sequentially."""
        for code in stock_codes:
            await self.load_historical_data(code)

    # ── Minute Candle Accumulation ───────────────────────────────────────

    def append_minute_candle(
        self, stock_code: str, candle: Dict[str, Any]
    ) -> None:
        """
        Append a completed minute candle to the in-memory buffer.

        Parameters
        ----------
        candle : dict
            Must contain: ``open``, ``high``, ``low``, ``close``,
            ``volume``, ``timestamp``.
        """
        if stock_code not in self._minute_candles:
            self._minute_candles[stock_code] = []
        self._minute_candles[stock_code].append(candle)

    def set_today_open(self, stock_code: str, open_price: float) -> None:
        """Record today's opening price (set once at 09:00:00 KST)."""
        self._today_open[stock_code] = open_price

    # ── Main Query Interface ─────────────────────────────────────────────

    def get_market_data(self, stock_code: str) -> MarketData:
        """
        Build an aggregated MarketData snapshot for *stock_code*.

        This is the **single entry point** for strategy engines to
        obtain all required market information.  No REST calls are made;
        all data comes from the in-memory cache and pre-loaded history.

        Returns
        -------
        MarketData
            Fully populated market data object.
        """
        # Realtime price
        price_data = self.cache.get_price(stock_code)
        current_price = price_data.price if price_data else 0.0
        current_volume = price_data.volume if price_data else 0
        change_pct = price_data.change_pct if price_data else 0.0
        price_ts = price_data.timestamp if price_data else None

        # Orderbook
        orderbook = self.cache.get_orderbook(stock_code)

        # Indicators (pre-computed from daily candles)
        indicators = self._indicators.get(stock_code, {})

        # Previous day
        prev_day = self._prev_day.get(stock_code, {})

        # Today's open
        today_open = self._today_open.get(stock_code, 0.0)

        # Minute candles
        minute_candles = self._minute_candles.get(stock_code, [])

        # Daily candles
        daily_candles = self._historical.get(stock_code, [])

        # VI state
        vi_state = self.vi_monitor.get_state(stock_code)
        vi_tradeable = self.vi_monitor.is_tradeable(stock_code)

        # Data freshness
        data_fresh = self.cache.is_data_fresh(stock_code)

        md = MarketData(
            stock_code=stock_code,
            current_price=current_price,
            current_volume=current_volume,
            change_pct=change_pct,
            orderbook=orderbook,
            indicators=indicators,
            prev_day=prev_day,
            today_open=today_open,
            minute_candles=minute_candles,
            daily_candles=daily_candles,
            vi_state=vi_state.value,
            vi_tradeable=vi_tradeable,
            data_fresh=data_fresh,
            price_timestamp=price_ts,
        )

        return md

    # ── Utility ──────────────────────────────────────────────────────────

    def get_indicator(self, stock_code: str, key: str) -> Any:
        """Shortcut to fetch a single pre-computed indicator value."""
        return self._indicators.get(stock_code, {}).get(key)

    def is_ready(self, stock_code: str) -> bool:
        """
        Return ``True`` if we have both historical data and fresh
        realtime data for *stock_code*.
        """
        has_history = stock_code in self._historical
        has_fresh = self.cache.is_data_fresh(stock_code)
        return has_history and has_fresh

    async def refresh_indicators(self, stock_code: str) -> None:
        """
        Re-compute indicators from cached historical data.

        Useful after appending new daily candles post-market.
        """
        daily = self._historical.get(stock_code)
        if daily:
            self._indicators[stock_code] = IndicatorCalculator.calculate_all(daily)
            logger.debug(
                "indicators_refreshed",
                stock_code=stock_code,
            )

    def clear_session_data(self) -> None:
        """
        Clear all intraday session data (called at end-of-day).

        Retains historical daily data and indicators.
        """
        self._minute_candles.clear()
        self._today_open.clear()
        self.cache.clear()
        logger.info("session_data_cleared")

    async def shutdown(self) -> None:
        """Graceful shutdown of all sub-components."""
        await self.vi_monitor.shutdown()
        self.cache.clear()
        logger.info("market_data_hub_shutdown")
