"""
KATS Triple Screen Strategy (S4)

Alexander Elder's Triple Screen Trading System adapted for Korean equities.

Three screens:
    Screen 1 (Weekly trend): MACD Histogram slope on weekly chart determines
        the primary trend direction.  Trade only in that direction.
    Screen 2 (Daily oscillator): Force Index (2-day EMA) identifies pullbacks
        within the trend.  In an uptrend, buy when Force Index goes negative.
    Screen 3 (Intraday entry): Trailing buy-stop technique on intraday chart
        for precise entry timing.

Entry: All three screens aligned.
Exit: Stop at previous swing low; targets +8 %, +15 %, trailing.
Focus: A-B grade stocks, 22.5 % base position.

References:
    - Alexander Elder, "Trading for a Living"
    - Alexander Elder, "Come Into My Trading Room"
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import structlog

from kats.strategy.base_strategy import (
    BaseStrategy,
    StockCandidate,
    StrategyCategory,
    TradeSignal,
)

logger = structlog.get_logger(__name__)


class TripleScreenStrategy(BaseStrategy):
    """Elder's Triple Screen trend-following strategy.

    Uses three timeframes (weekly, daily, intraday) to align trend,
    pullback, and entry timing.

    Category: BULL (STRONG_BULL, BULL regimes).
    """

    def __init__(self) -> None:
        super().__init__("S4", StrategyCategory.BULL)
        self.params: Dict[str, Any] = {
            # Screen 1 - Weekly MACD-H
            "macd_fast": 12,
            "macd_slow": 26,
            "macd_signal": 9,
            # Screen 2 - Force Index EMA period
            "force_index_period": 2,
            # Screen 3 - Entry
            "trailing_buy_stop_bars": 3,
            # Risk
            "stop_loss_pct": 5.0,
            "target_1_pct": 8.0,
            "target_2_pct": 15.0,
            "grade_target": ["A", "B"],
            "position_pct": 22.5,
        }

    # ── Screen 1: Weekly trend via MACD Histogram ─────────────────────────

    @staticmethod
    def _screen1_weekly_trend(indicators: Dict[str, Any]) -> Optional[str]:
        """Determine weekly trend direction from MACD Histogram.

        Returns:
            ``"UP"`` if the MACD-H slope is rising (bullish).
            ``"DOWN"`` if the MACD-H slope is falling (bearish).
            ``None`` if data is insufficient.
        """
        macd_h = indicators.get("weekly_macd_histogram")
        if macd_h is None:
            # Fallback: use daily MACD-H if weekly is unavailable
            macd_h = indicators.get("macd_histogram")

        if macd_h is None:
            return None

        try:
            if hasattr(macd_h, "__len__") and len(macd_h) >= 2:
                values = list(macd_h)
                if values[-1] > values[-2]:
                    return "UP"
                else:
                    return "DOWN"
            else:
                # Scalar -- use sign
                val = float(macd_h) if not hasattr(macd_h, "item") else macd_h.item()
                return "UP" if val > 0 else "DOWN"
        except (TypeError, ValueError, IndexError):
            return None

    # ── Screen 2: Daily oscillator via Force Index ────────────────────────

    @staticmethod
    def _screen2_force_index(
        daily_prices: Any,
        period: int = 2,
    ) -> Optional[float]:
        """Compute the Force Index (EMA of price-change * volume).

        Returns the latest Force Index value, or ``None``.
        """
        try:
            closes = daily_prices["close"].values
            volumes = daily_prices["volume"].values
        except (KeyError, AttributeError):
            return None

        if len(closes) < period + 2:
            return None

        # Force = (close - prev_close) * volume
        price_change = np.diff(closes)
        raw_force = price_change * volumes[1:]

        # EMA smoothing
        alpha = 2.0 / (period + 1)
        ema = raw_force[0]
        for val in raw_force[1:]:
            ema = alpha * val + (1 - alpha) * ema

        return float(ema)

    # ── Screen 3: Trailing buy-stop entry ─────────────────────────────────

    @staticmethod
    def _screen3_trailing_buy_stop(
        minute_candles: Any,
        lookback_bars: int = 3,
    ) -> Optional[float]:
        """Calculate trailing buy-stop price.

        The buy-stop is placed at the highest high of the last
        *lookback_bars* candles.  Once price exceeds this level, the entry
        is confirmed.
        """
        try:
            highs = minute_candles["high"].values[-lookback_bars:]
        except (KeyError, AttributeError, IndexError):
            return None

        if len(highs) < lookback_bars:
            return None

        return float(np.max(highs))

    # ── Previous swing low for stop ───────────────────────────────────────

    @staticmethod
    def _find_swing_low(daily_prices: Any, lookback: int = 20) -> float:
        """Find the lowest low in the last *lookback* daily bars."""
        try:
            lows = daily_prices["low"].values[-lookback:]
        except (KeyError, AttributeError, IndexError):
            return 0.0
        return float(np.min(lows)) if len(lows) > 0 else 0.0

    # ── Scan ──────────────────────────────────────────────────────────────

    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Filter to A-B grade stocks in an established uptrend."""
        filtered: List[StockCandidate] = []
        for c in candidates:
            if c.grade not in self.params["grade_target"]:
                continue
            # Must be above MA50 (basic uptrend filter)
            if c.price < c.ma_50:
                continue
            # Reasonable RS rank
            if c.rs_rank < 50:
                continue
            filtered.append(c)

        self.log.info("scan_complete", strategy="S4", matched=len(filtered))
        return filtered

    # ── Signal generation ─────────────────────────────────────────────────

    async def generate_signal(
        self,
        stock: StockCandidate,
        market_data: Dict[str, Any],
    ) -> Optional[TradeSignal]:
        indicators: Dict[str, Any] = market_data.get("indicators", {})
        daily_prices = market_data.get("daily_prices")
        minute_candles = market_data.get("minute_candles")
        current_price: float = market_data["current_price"]

        # Screen 1: Weekly trend must be UP
        trend = self._screen1_weekly_trend(indicators)
        if trend != "UP":
            return None

        # Screen 2: Force Index should be negative (pullback in uptrend)
        force_idx = self._screen2_force_index(
            daily_prices, self.params["force_index_period"]
        )
        if force_idx is None or force_idx >= 0:
            # Force Index positive means no pullback yet
            return None

        # Screen 3: Price must have broken the trailing buy-stop
        buy_stop = self._screen3_trailing_buy_stop(
            minute_candles, self.params["trailing_buy_stop_bars"]
        )
        if buy_stop is None or current_price < buy_stop:
            return None

        # Stop loss at previous swing low or percentage, whichever is tighter
        swing_low = self._find_swing_low(daily_prices)
        pct_stop = current_price * (1 - self.params["stop_loss_pct"] / 100)
        stop_loss = max(swing_low, pct_stop)

        self.log.info(
            "signal_generated",
            stock=stock.stock_code,
            weekly_trend=trend,
            force_index=round(force_idx, 2),
            buy_stop=round(buy_stop),
        )

        return TradeSignal(
            stock_code=stock.stock_code,
            action="BUY",
            strategy_code="S4",
            entry_price=current_price,
            stop_loss=stop_loss,
            target_prices=[
                current_price * (1 + self.params["target_1_pct"] / 100),
                current_price * (1 + self.params["target_2_pct"] / 100),
                0,  # trailing stop
            ],
            position_pct=self._adjust_position(stock.confidence, stock.grade),
            confidence=stock.confidence,
            reason=(
                f"Triple Screen: 주간 상승추세(MACD-H), "
                f"일봉 눌림(FI={force_idx:,.0f}), "
                f"분봉 돌파({buy_stop:,.0f}원)"
            ),
            indicators_snapshot=self._capture_snapshot(indicators),
        )

    # ── Exit rules ────────────────────────────────────────────────────────

    def get_exit_rules(self) -> Dict[str, Any]:
        return {
            "stop_loss_pct": self.params["stop_loss_pct"],
            "target_prices_pct": [
                self.params["target_1_pct"],
                self.params["target_2_pct"],
            ],
            "trailing_stop": True,
            "trailing_stop_pct": 4.0,
            "time_exit": None,
            "max_holding_hours": None,
        }
