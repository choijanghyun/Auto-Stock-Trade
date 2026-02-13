"""
KATS Range Trading Strategy (B3)

Box-range (support/resistance) trading strategy using Bollinger Band
squeeze detection and candlestick pattern confirmation.

Core logic:
    1. Detect a horizontal support/resistance channel (box).
    2. Bollinger Band width narrows (squeeze) indicating low volatility.
    3. Buy at support (lower BB / channel bottom) with bullish reversal candle.
    4. Sell at resistance (upper BB / channel top).

Entry: Price at support + BB squeeze + bullish reversal.
Exit: Price at resistance OR stop below support.
Focus: A-B grade stocks in range-bound markets, 12.5 % base position.

References:
    - Steve Nison, "Japanese Candlestick Charting Techniques"
    - John Murphy, "Technical Analysis of the Financial Markets"
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import structlog

from kats.strategy.base_strategy import (
    BaseStrategy,
    StockCandidate,
    StrategyCategory,
    TradeSignal,
)

logger = structlog.get_logger(__name__)


class RangeTradingStrategy(BaseStrategy):
    """Support/resistance range trading with BB squeeze confirmation.

    Best suited for sideways markets where stocks oscillate within a
    well-defined price box.

    Category: BEAR (also used in SIDEWAYS via StrategySelector mapping).
    """

    def __init__(self) -> None:
        super().__init__("B3", StrategyCategory.BEAR)
        self.params: Dict[str, Any] = {
            "channel_lookback_days": 30,
            "channel_max_range_pct": 15,
            "bb_period": 20,
            "bb_squeeze_threshold": 0.04,  # BB width / price ratio
            "support_proximity_pct": 1.5,
            "resistance_proximity_pct": 1.5,
            "stop_loss_pct": 3.0,
            "target_pct": 5.0,
            "grade_target": ["A", "B"],
            "position_pct": 12.5,
        }

    # ── Channel detection ─────────────────────────────────────────────────

    @staticmethod
    def _detect_channel(
        daily_prices: Any,
        lookback: int = 30,
        max_range_pct: float = 15.0,
    ) -> Optional[Tuple[float, float]]:
        """Detect a horizontal price channel (support, resistance).

        Returns:
            Tuple of (support, resistance) prices, or ``None`` if no
            valid channel is found.

        A channel is valid when the range (resistance - support) is
        <= *max_range_pct* of the midpoint.
        """
        try:
            highs = daily_prices["high"].values[-lookback:]
            lows = daily_prices["low"].values[-lookback:]
        except (KeyError, AttributeError, IndexError):
            return None

        if len(highs) < 10:
            return None

        resistance = float(np.max(highs))
        support = float(np.min(lows))

        midpoint = (resistance + support) / 2
        if midpoint <= 0:
            return None

        range_pct = (resistance - support) / midpoint * 100
        if range_pct > max_range_pct:
            return None

        return (support, resistance)

    # ── Bollinger Band squeeze ────────────────────────────────────────────

    @staticmethod
    def _is_bb_squeeze(
        indicators: Dict[str, Any],
        current_price: float,
        threshold: float = 0.04,
    ) -> bool:
        """Check if Bollinger Bands are in a squeeze (low volatility).

        A squeeze occurs when BB width (upper - lower) / price < threshold.
        """
        bb_upper = indicators.get("bb_upper", 0)
        bb_lower = indicators.get("bb_lower", 0)

        if current_price <= 0 or bb_upper <= 0 or bb_lower <= 0:
            return False

        bb_width_ratio = (bb_upper - bb_lower) / current_price
        return bb_width_ratio <= threshold

    # ── Bullish reversal candle ───────────────────────────────────────────

    @staticmethod
    def _is_bullish_reversal(minute_candles: Any) -> bool:
        """Detect a bullish reversal pattern in recent candles.

        Simple check: last candle is bullish (close > open) and its body
        covers at least 60 % of the candle range (strong close).
        """
        try:
            opens = minute_candles["open"].values
            closes = minute_candles["close"].values
            highs = minute_candles["high"].values
            lows = minute_candles["low"].values
        except (KeyError, AttributeError):
            return False

        if len(opens) < 1:
            return False

        o, c, h, l = opens[-1], closes[-1], highs[-1], lows[-1]
        if c <= o:
            return False

        candle_range = h - l
        if candle_range <= 0:
            return False

        body_ratio = (c - o) / candle_range
        return body_ratio >= 0.6

    # ── Scan ──────────────────────────────────────────────────────────────

    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Filter to range-bound A-B grade stocks."""
        filtered: List[StockCandidate] = []
        for c in candidates:
            if c.grade not in self.params["grade_target"]:
                continue
            # Prefer stocks with low trend score (range-bound)
            if c.trend_score > 50:
                continue
            filtered.append(c)

        self.log.info("scan_complete", strategy="B3", matched=len(filtered))
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

        # 1. Detect price channel
        channel = self._detect_channel(
            daily_prices,
            self.params["channel_lookback_days"],
            self.params["channel_max_range_pct"],
        )
        if channel is None:
            return None

        support, resistance = channel

        # 2. BB squeeze confirmation
        squeeze = self._is_bb_squeeze(
            indicators, current_price, self.params["bb_squeeze_threshold"]
        )

        # 3. Price near support (buy zone)
        if support <= 0:
            return None
        distance_to_support_pct = (current_price - support) / support * 100
        near_support = distance_to_support_pct <= self.params["support_proximity_pct"]

        if not near_support:
            # Check near resistance for sell signal
            if resistance > 0:
                distance_to_resistance_pct = (
                    (resistance - current_price) / resistance * 100
                )
                if distance_to_resistance_pct <= self.params["resistance_proximity_pct"]:
                    # Sell signal at resistance
                    return TradeSignal(
                        stock_code=stock.stock_code,
                        action="SELL",
                        strategy_code="B3",
                        entry_price=current_price,
                        stop_loss=resistance * 1.02,
                        target_prices=[support * 1.01],
                        position_pct=self.params["position_pct"],
                        confidence=min(stock.confidence, 3),
                        reason=(
                            f"레인지 매도: 저항 {resistance:,.0f}원 도달, "
                            f"채널 [{support:,.0f} ~ {resistance:,.0f}]"
                        ),
                        indicators_snapshot=self._capture_snapshot(indicators),
                    )
            return None

        # 4. Bullish reversal confirmation at support
        if not self._is_bullish_reversal(minute_candles):
            # If BB squeeze is present, lower the bar
            if not squeeze:
                return None

        stop_loss = support * (1 - self.params["stop_loss_pct"] / 100)

        self.log.info(
            "signal_generated",
            stock=stock.stock_code,
            support=round(support),
            resistance=round(resistance),
            squeeze=squeeze,
        )

        return TradeSignal(
            stock_code=stock.stock_code,
            action="BUY",
            strategy_code="B3",
            entry_price=current_price,
            stop_loss=stop_loss,
            target_prices=[
                resistance * 0.99,  # Just below resistance
            ],
            position_pct=self._adjust_position(stock.confidence, stock.grade),
            confidence=min(stock.confidence, 3),
            reason=(
                f"레인지 매수: 지지 {support:,.0f}원 반등, "
                f"채널 [{support:,.0f} ~ {resistance:,.0f}]"
                f"{', BB 스퀴즈' if squeeze else ''}"
            ),
            indicators_snapshot=self._capture_snapshot(indicators),
        )

    # ── Exit rules ────────────────────────────────────────────────────────

    def get_exit_rules(self) -> Dict[str, Any]:
        return {
            "stop_loss_pct": self.params["stop_loss_pct"],
            "target_prices_pct": [self.params["target_pct"]],
            "trailing_stop": False,
            "trailing_stop_pct": None,
            "time_exit": None,
            "max_holding_hours": None,
        }
