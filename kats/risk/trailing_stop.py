"""
KATS Multi-Method Trailing Stop

Supports five trailing-stop methods that adapt to different market conditions:
    1. FIXED_PCT      -- Fixed percentage drop from highest price
    2. MOVING_AVG     -- Close below a moving average
    3. ATR_BASED      -- ATR-multiple trailing from highest price
    4. CANDLE_PATTERN -- Bearish candlestick pattern detection
    5. VOLUME_ANOMALY -- Abnormal volume with bearish price action

References:
    - Minervini, trailing-stop tightening technique
    - Elder, ATR-based chandelier exit
    - Nison, Japanese candlestick patterns
"""

from __future__ import annotations

from enum import Enum, unique
from typing import Any, Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger(__name__)


@unique
class TrailingStopMethod(str, Enum):
    """Trailing stop calculation method."""

    FIXED_PCT = "FIXED_PCT"
    MOVING_AVG = "MOVING_AVG"
    ATR_BASED = "ATR_BASED"
    CANDLE_PATTERN = "CANDLE_PATTERN"
    VOLUME_ANOMALY = "VOLUME_ANOMALY"


# ── Default parameters per method ──────────────────────────────────────

_DEFAULT_PARAMS: Dict[TrailingStopMethod, Dict[str, Any]] = {
    TrailingStopMethod.FIXED_PCT: {"trail_pct": 5.0},
    TrailingStopMethod.MOVING_AVG: {"ma_period": 20},
    TrailingStopMethod.ATR_BASED: {"atr_period": 14, "atr_multiplier": 3.0},
    TrailingStopMethod.CANDLE_PATTERN: {},
    TrailingStopMethod.VOLUME_ANOMALY: {"volume_multiplier": 2.5},
}


class TrailingStop:
    """
    Multi-method trailing stop manager for a single position.

    Usage::

        ts = TrailingStop(method=TrailingStopMethod.ATR_BASED)
        triggered, reason = ts.update_and_check(
            current_price=48_000,
            market_data={
                "high": [50000, 49500, 49800],
                "low": [49000, 48800, 49000],
                "close": [49500, 49200, 49300],
                "volume": [100000, 120000, 95000],
                "open": [49800, 49600, 49100],
            },
        )
    """

    def __init__(
        self,
        method: TrailingStopMethod = TrailingStopMethod.FIXED_PCT,
        params: Dict[str, Any] | None = None,
        entry_price: int = 0,
    ) -> None:
        self._method = method
        self._params: Dict[str, Any] = {
            **_DEFAULT_PARAMS.get(method, {}),
            **(params or {}),
        }
        self._entry_price = entry_price
        self._highest_price: float = float(entry_price)
        self._active = True

        logger.info(
            "trailing_stop_initialized",
            method=method.value,
            params=self._params,
            entry_price=entry_price,
        )

    # ── Properties ─────────────────────────────────────────────────────

    @property
    def highest_price(self) -> float:
        return self._highest_price

    @property
    def method(self) -> TrailingStopMethod:
        return self._method

    @property
    def is_active(self) -> bool:
        return self._active

    def deactivate(self) -> None:
        """Deactivate the trailing stop (e.g., position closed)."""
        self._active = False

    # ── Main API ───────────────────────────────────────────────────────

    def update_and_check(
        self,
        current_price: float,
        market_data: Dict[str, List[float]],
    ) -> Tuple[bool, str]:
        """
        Update highest price and check whether the stop has been triggered.

        Args:
            current_price: Latest trade price.
            market_data: Dict with keys ``high``, ``low``, ``close``,
                ``open``, ``volume`` -- each a list of recent values
                (most recent last).

        Returns:
            (triggered: bool, reason: str)
        """
        if not self._active:
            return False, ""

        # Track highest price
        if current_price > self._highest_price:
            self._highest_price = current_price

        dispatch = {
            TrailingStopMethod.FIXED_PCT: self._check_fixed_pct,
            TrailingStopMethod.MOVING_AVG: self._check_moving_avg,
            TrailingStopMethod.ATR_BASED: self._check_atr_based,
            TrailingStopMethod.CANDLE_PATTERN: self._check_candle_pattern,
            TrailingStopMethod.VOLUME_ANOMALY: self._check_volume_anomaly,
        }

        handler = dispatch[self._method]
        triggered, reason = handler(current_price, market_data)

        if triggered:
            logger.warning(
                "trailing_stop_triggered",
                method=self._method.value,
                current_price=current_price,
                highest_price=self._highest_price,
                reason=reason,
            )
            self._active = False

        return triggered, reason

    # ── Method implementations ─────────────────────────────────────────

    def _check_fixed_pct(
        self,
        current_price: float,
        market_data: Dict[str, List[float]],
    ) -> Tuple[bool, str]:
        """
        FIXED_PCT: stop triggers when price drops ``trail_pct``% from the
        highest observed price.
        """
        trail_pct = self._params["trail_pct"]
        stop_price = self._highest_price * (1 - trail_pct / 100.0)

        if current_price <= stop_price:
            drop_pct = (1 - current_price / self._highest_price) * 100
            return True, (
                f"FIXED_PCT trailing stop: price {current_price:,.0f} "
                f"<= stop {stop_price:,.0f} "
                f"({drop_pct:.1f}% from high {self._highest_price:,.0f})"
            )
        return False, ""

    def _check_moving_avg(
        self,
        current_price: float,
        market_data: Dict[str, List[float]],
    ) -> Tuple[bool, str]:
        """
        MOVING_AVG: stop triggers when the latest close drops below the
        simple moving average of ``ma_period`` bars.
        """
        period = self._params["ma_period"]
        closes = market_data.get("close", [])

        if len(closes) < period:
            return False, ""

        recent_closes = closes[-period:]
        ma_value = sum(recent_closes) / period

        if current_price < ma_value:
            return True, (
                f"MOVING_AVG stop: price {current_price:,.0f} "
                f"< MA({period}) {ma_value:,.0f}"
            )
        return False, ""

    def _check_atr_based(
        self,
        current_price: float,
        market_data: Dict[str, List[float]],
    ) -> Tuple[bool, str]:
        """
        ATR_BASED (chandelier exit): stop at highest_price - multiplier * ATR.
        """
        atr_period = self._params["atr_period"]
        multiplier = self._params["atr_multiplier"]

        highs = market_data.get("high", [])
        lows = market_data.get("low", [])
        closes = market_data.get("close", [])

        if len(highs) < atr_period + 1 or len(lows) < atr_period + 1:
            return False, ""

        # Calculate True Range over the last ``atr_period`` bars
        true_ranges: List[float] = []
        for i in range(-atr_period, 0):
            high = highs[i]
            low = lows[i]
            prev_close = closes[i - 1]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            true_ranges.append(tr)

        atr = sum(true_ranges) / len(true_ranges)
        stop_price = self._highest_price - multiplier * atr

        if current_price <= stop_price:
            return True, (
                f"ATR_BASED stop: price {current_price:,.0f} "
                f"<= stop {stop_price:,.0f} "
                f"(high {self._highest_price:,.0f} - {multiplier}*ATR {atr:,.0f})"
            )
        return False, ""

    def _check_candle_pattern(
        self,
        current_price: float,
        market_data: Dict[str, List[float]],
    ) -> Tuple[bool, str]:
        """
        CANDLE_PATTERN: detect bearish engulfing or shooting star on the
        most recent completed candle.
        """
        opens = market_data.get("open", [])
        closes = market_data.get("close", [])
        highs = market_data.get("high", [])
        lows = market_data.get("low", [])

        if len(opens) < 2 or len(closes) < 2:
            return False, ""

        # Latest completed candle (index -1) and prior candle (index -2)
        o1, c1 = opens[-2], closes[-2]  # prior candle
        o2, c2 = opens[-1], closes[-1]  # latest candle
        h2, l2 = highs[-1], lows[-1]

        # ── Bearish Engulfing ──────────────────────────────────────────
        prior_bullish = c1 > o1
        latest_bearish = c2 < o2
        engulfs = o2 >= c1 and c2 <= o1

        if prior_bullish and latest_bearish and engulfs:
            return True, (
                f"CANDLE_PATTERN: Bearish Engulfing detected at "
                f"price {current_price:,.0f}"
            )

        # ── Shooting Star ──────────────────────────────────────────────
        body = abs(c2 - o2)
        candle_range = h2 - l2 if h2 != l2 else 1
        upper_shadow = h2 - max(o2, c2)
        lower_shadow = min(o2, c2) - l2

        is_shooting_star = (
            candle_range > 0
            and upper_shadow >= 2 * body
            and lower_shadow <= body * 0.3
            and body / candle_range <= 0.35
        )

        if is_shooting_star:
            return True, (
                f"CANDLE_PATTERN: Shooting Star detected at "
                f"price {current_price:,.0f}"
            )

        return False, ""

    def _check_volume_anomaly(
        self,
        current_price: float,
        market_data: Dict[str, List[float]],
    ) -> Tuple[bool, str]:
        """
        VOLUME_ANOMALY: large volume spike combined with a bearish candle.
        """
        volumes = market_data.get("volume", [])
        opens = market_data.get("open", [])
        closes = market_data.get("close", [])
        multiplier = self._params["volume_multiplier"]

        # Need at least 20 bars for average + 1 current bar
        if len(volumes) < 21:
            return False, ""

        avg_volume = sum(volumes[-21:-1]) / 20
        latest_volume = volumes[-1]

        if avg_volume <= 0:
            return False, ""

        volume_ratio = latest_volume / avg_volume
        latest_bearish = closes[-1] < opens[-1]

        if volume_ratio >= multiplier and latest_bearish:
            return True, (
                f"VOLUME_ANOMALY: volume {latest_volume:,.0f} = "
                f"{volume_ratio:.1f}x avg ({avg_volume:,.0f}), "
                f"bearish candle at {current_price:,.0f}"
            )

        return False, ""

    # ── Utility ────────────────────────────────────────────────────────

    def get_current_stop_price(self) -> Optional[float]:
        """Return the current trailing stop price for FIXED_PCT method."""
        if self._method == TrailingStopMethod.FIXED_PCT:
            trail_pct = self._params["trail_pct"]
            return self._highest_price * (1 - trail_pct / 100.0)
        return None

    def __repr__(self) -> str:
        return (
            f"TrailingStop(method={self._method.value}, "
            f"highest={self._highest_price:,.0f}, "
            f"active={self._active})"
        )
