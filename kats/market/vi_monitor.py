"""
KATS VIMonitor - Volatility Interruption (VI) Monitor

Korean market specific:
  - Static VI  : triggered when price moves +-10% from previous close.
  - Dynamic VI : triggered on sudden intra-session price changes.
  - When VI is triggered the stock enters a 2-minute trading halt.
  - After release a 30-second cooling observation period applies before
    new orders should be placed.

This module:
  1. Tracks VI trigger prices (static upper/lower, dynamic) per stock.
  2. Receives WebSocket VI events and transitions per-stock state machines.
  3. Provides ``check_vi_proximity`` so breakout strategies can avoid
     placing orders that would collide with a VI boundary.
"""

from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import Any, Dict, Optional

import structlog

from kats.market.realtime_cache import RealtimeCache

logger = structlog.get_logger(__name__)


class VIState(Enum):
    """Per-stock VI state machine states."""

    NORMAL = "NORMAL"
    WARNING = "WARNING"            # price within 1% of VI trigger
    VI_TRIGGERED = "VI_TRIGGERED"  # 2-min trading halt active
    COOLING = "COOLING"            # post-release 30-sec observation


class VIMonitor:
    """
    Realtime VI state tracker and order gate.

    Parameters
    ----------
    cache : RealtimeCache
        Shared in-memory cache (for reading latest prices).
    cooling_seconds : int
        Observation window after VI release before returning to NORMAL.
        Default 30 seconds per KRX rules.
    proximity_pct : float
        Distance threshold (%) to VI trigger price for issuing a WARNING.
        Default 1.0%.
    """

    def __init__(
        self,
        cache: RealtimeCache,
        cooling_seconds: int = 30,
        proximity_pct: float = 1.0,
    ) -> None:
        self.cache = cache
        self.cooling_seconds = cooling_seconds
        self.proximity_pct = proximity_pct

        # per-stock tracking dicts
        self._vi_prices: Dict[str, Dict[str, float]] = {}
        # {stock_code: {"reference_price", "static_upper", "static_lower", "dynamic"}}

        self._vi_states: Dict[str, VIState] = {}
        self._vi_triggered_at: Dict[str, float] = {}
        self._vi_released_at: Dict[str, float] = {}

        # background tasks for cooling timers (keyed by stock_code)
        self._cooling_tasks: Dict[str, asyncio.Task] = {}

    # ── WebSocket Callback ───────────────────────────────────────────────

    async def on_vi_data(self, stock_code: str, data: dict) -> None:
        """
        WebSocket callback for VI information (H0STVI0).

        Parameters
        ----------
        stock_code : str
            6-digit KRX stock code.
        data : dict
            - vi_cls_code : "1" = VI triggered, "2" = VI released
            - vi_stnd_prc : VI reference price (typically previous close)
        """
        vi_cls = data.get("vi_cls_code", "")
        ref_price_raw = data.get("vi_stnd_prc")

        # Update trigger price boundaries whenever reference price is available
        if ref_price_raw:
            ref_price = float(ref_price_raw)
            self._vi_prices[stock_code] = {
                "reference_price": ref_price,
                "static_upper": ref_price * 1.10,  # +10%
                "static_lower": ref_price * 0.90,  # -10%
                "dynamic": float(data.get("vi_dyn_prc", 0)) or 0.0,
            }

        if vi_cls == "1":
            # ── VI Triggered ─────────────────────────────────────────
            self._vi_states[stock_code] = VIState.VI_TRIGGERED
            self._vi_triggered_at[stock_code] = time.monotonic()

            # Cancel any pending cooling timer
            self._cancel_cooling_task(stock_code)

            logger.warning(
                "vi_triggered",
                stock_code=stock_code,
                reference_price=self._vi_prices.get(stock_code, {}).get("reference_price"),
                msg=f"{stock_code} VI triggered -- 2-min trading halt",
            )

            # Also update the shared RealtimeCache VI status
            await self.cache.on_vi_update(stock_code, data)

        elif vi_cls == "2":
            # ── VI Released ──────────────────────────────────────────
            self._vi_states[stock_code] = VIState.COOLING
            self._vi_released_at[stock_code] = time.monotonic()

            logger.info(
                "vi_released",
                stock_code=stock_code,
                cooling_seconds=self.cooling_seconds,
                msg=f"{stock_code} VI released -- {self.cooling_seconds}s cooling observation",
            )

            # Schedule transition back to NORMAL after cooling period
            self._cancel_cooling_task(stock_code)
            task = asyncio.create_task(
                self._transition_to_normal(stock_code, delay=self.cooling_seconds)
            )
            self._cooling_tasks[stock_code] = task

            await self.cache.on_vi_update(stock_code, data)

        else:
            # Informational update (reference price change, etc.)
            logger.debug(
                "vi_info_update",
                stock_code=stock_code,
                vi_cls_code=vi_cls,
                vi_prices=self._vi_prices.get(stock_code),
            )

    # ── Query Methods ────────────────────────────────────────────────────

    def get_state(self, stock_code: str) -> VIState:
        """Return current VI state for *stock_code*."""
        return self._vi_states.get(stock_code, VIState.NORMAL)

    def get_vi_prices(self, stock_code: str) -> Optional[Dict[str, float]]:
        """Return VI trigger price boundaries, or ``None`` if unknown."""
        return self._vi_prices.get(stock_code)

    def check_vi_proximity(self, stock_code: str, target_price: float) -> Dict[str, Any]:
        """
        Evaluate whether *target_price* can be safely used for an order.

        Returns
        -------
        dict
            - allow_order : bool -- False means the order must be blocked.
            - reason : str       -- Human-readable explanation (set when blocked).
            - warning : str      -- Advisory message (set when close to VI).
            - vi_state : str     -- Current VI state label.
        """
        state = self._vi_states.get(stock_code, VIState.NORMAL)

        # Hard blocks
        if state == VIState.VI_TRIGGERED:
            return {
                "allow_order": False,
                "reason": f"{stock_code} VI triggered -- trading halted for 2 minutes",
                "warning": None,
                "vi_state": state.value,
            }

        if state == VIState.COOLING:
            elapsed = time.monotonic() - self._vi_released_at.get(stock_code, 0)
            remaining = max(0, self.cooling_seconds - elapsed)
            return {
                "allow_order": False,
                "reason": (
                    f"{stock_code} VI just released -- "
                    f"{remaining:.0f}s cooling observation remaining"
                ),
                "warning": None,
                "vi_state": state.value,
            }

        # Proximity check against static VI upper bound
        vi_prices = self._vi_prices.get(stock_code)
        if vi_prices and target_price > 0:
            upper = vi_prices["static_upper"]
            lower = vi_prices["static_lower"]

            # Check proximity to upper VI
            if upper > 0:
                proximity_upper = abs(target_price - upper) / upper * 100
                if proximity_upper < self.proximity_pct:
                    return {
                        "allow_order": True,
                        "reason": None,
                        "warning": (
                            f"Target price {target_price:,.0f} is {proximity_upper:.2f}% "
                            f"from static VI upper ({upper:,.0f}) -- proceed with caution"
                        ),
                        "vi_state": VIState.WARNING.value,
                    }

            # Check proximity to lower VI
            if lower > 0:
                proximity_lower = abs(target_price - lower) / lower * 100
                if proximity_lower < self.proximity_pct:
                    return {
                        "allow_order": True,
                        "reason": None,
                        "warning": (
                            f"Target price {target_price:,.0f} is {proximity_lower:.2f}% "
                            f"from static VI lower ({lower:,.0f}) -- proceed with caution"
                        ),
                        "vi_state": VIState.WARNING.value,
                    }

        # All clear
        return {
            "allow_order": True,
            "reason": None,
            "warning": None,
            "vi_state": VIState.NORMAL.value,
        }

    def is_tradeable(self, stock_code: str) -> bool:
        """Quick boolean check: can we trade *stock_code* right now?"""
        state = self._vi_states.get(stock_code, VIState.NORMAL)
        return state in (VIState.NORMAL, VIState.WARNING)

    # ── Internal Helpers ─────────────────────────────────────────────────

    async def _transition_to_normal(self, stock_code: str, delay: int) -> None:
        """After *delay* seconds, set state back to NORMAL."""
        try:
            await asyncio.sleep(delay)
            self._vi_states[stock_code] = VIState.NORMAL
            self._cooling_tasks.pop(stock_code, None)
            logger.info(
                "vi_cooling_complete",
                stock_code=stock_code,
                msg=f"{stock_code} cooling period ended -- state is NORMAL",
            )
        except asyncio.CancelledError:
            # Task was cancelled because a new VI event arrived
            logger.debug(
                "vi_cooling_cancelled",
                stock_code=stock_code,
            )

    def _cancel_cooling_task(self, stock_code: str) -> None:
        """Cancel a pending cooling timer if one exists."""
        task = self._cooling_tasks.pop(stock_code, None)
        if task is not None and not task.done():
            task.cancel()

    # ── Lifecycle ────────────────────────────────────────────────────────

    def initialize_vi_prices(self, stock_code: str, prev_close: float) -> None:
        """
        Pre-seed VI boundaries from previous close (called at market open).

        Static VI = previous close +- 10%.
        """
        if prev_close <= 0:
            return
        self._vi_prices[stock_code] = {
            "reference_price": prev_close,
            "static_upper": prev_close * 1.10,
            "static_lower": prev_close * 0.90,
            "dynamic": 0.0,
        }
        self._vi_states.setdefault(stock_code, VIState.NORMAL)
        logger.debug(
            "vi_prices_initialized",
            stock_code=stock_code,
            prev_close=prev_close,
            static_upper=prev_close * 1.10,
            static_lower=prev_close * 0.90,
        )

    async def shutdown(self) -> None:
        """Cancel all background tasks for clean shutdown."""
        for stock_code, task in list(self._cooling_tasks.items()):
            if not task.done():
                task.cancel()
        self._cooling_tasks.clear()
        logger.info("vi_monitor_shutdown")
