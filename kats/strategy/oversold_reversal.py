"""
KATS Oversold Reversal Strategy (B4)

Contrarian mean-reversion strategy that buys deeply oversold defensive
stocks for a short-term rebound.

Core logic:
    1. RSI(14) < 25 (extreme oversold).
    2. Price touches or penetrates the lower Bollinger Band.
    3. Volume spike on the reversal candle (1.5x average).
    4. Target defensive sectors: utilities, pharmaceuticals, telecom.

Entry: RSI < 25 + BB lower touch + volume spike reversal candle.
Exit: Target +5 %; stop -3 %; 2-day max hold.
Focus: A grade defensive stocks only, 12.5 % base position.

References:
    - Brett Steenbarger, "The Psychology of Trading"
    - Brett Steenbarger, "Enhancing Trader Performance"
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

import structlog

from kats.strategy.base_strategy import (
    BaseStrategy,
    StockCandidate,
    StrategyCategory,
    TradeSignal,
)

logger = structlog.get_logger(__name__)

# Defensive sectors in KRX classification (Korean sector names)
DEFENSIVE_SECTORS: Set[str] = {
    "전기가스업",         # Utilities
    "전력",
    "가스",
    "의약품",             # Pharmaceuticals
    "의료정밀",
    "통신업",             # Telecom
    "통신",
    "음식료품",           # Food & Beverage (defensive consumer)
    "유틸리티",
    "헬스케어",
    "제약",
    "바이오",
}


class OversoldReversalStrategy(BaseStrategy):
    """Contrarian oversold reversal on defensive stocks.

    Targets extreme oversold conditions (RSI < 25) in defensive sectors
    that have a structural floor due to regulated earnings or essential
    demand.

    Category: BEAR (BEAR, STRONG_BEAR regimes).
    """

    def __init__(self) -> None:
        super().__init__("B4", StrategyCategory.BEAR)
        self.params: Dict[str, Any] = {
            "rsi_threshold": 25,
            "bb_lower_touch_pct": 0.5,
            "volume_spike_ratio": 1.5,
            "target_pct": 5.0,
            "stop_loss_pct": 3.0,
            "max_holding_days": 2,
            "grade_target": ["A"],
            "position_pct": 12.5,
            "defensive_sectors": DEFENSIVE_SECTORS,
        }

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _is_bb_lower_touch(
        current_price: float,
        bb_lower: float,
        tolerance_pct: float = 0.5,
    ) -> bool:
        """Check if price is at or below the lower Bollinger Band."""
        if bb_lower <= 0:
            return False
        distance_pct = (current_price - bb_lower) / bb_lower * 100
        return distance_pct <= tolerance_pct

    @staticmethod
    def _detect_reversal_candle(
        minute_candles: Any,
        avg_minute_vol: float,
        spike_ratio: float,
    ) -> bool:
        """Detect a bullish reversal candle with a volume spike.

        Conditions:
            * Bullish candle (close > open).
            * Long lower shadow (wick >= body, indicating buying pressure).
            * Volume >= spike_ratio * average minute volume.
        """
        try:
            opens = minute_candles["open"].values
            closes = minute_candles["close"].values
            highs = minute_candles["high"].values
            lows = minute_candles["low"].values
            volumes = minute_candles["volume"].values
        except (KeyError, AttributeError):
            return False

        if len(opens) < 1:
            return False

        o, c, h, l, v = opens[-1], closes[-1], highs[-1], lows[-1], volumes[-1]

        # Must be bullish
        if c <= o:
            return False

        body = c - o
        lower_shadow = o - l  # distance from open to low

        # Lower shadow should be significant (buying absorbed selling)
        if body <= 0 or lower_shadow < body * 0.5:
            return False

        # Volume spike
        if float(v) < avg_minute_vol * spike_ratio:
            return False

        return True

    def _is_defensive_sector(self, sector: str) -> bool:
        """Check if the stock's sector qualifies as defensive."""
        for ds in self.params["defensive_sectors"]:
            if ds in sector:
                return True
        return False

    # ── Scan ──────────────────────────────────────────────────────────────

    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Filter to A-grade defensive-sector stocks."""
        filtered: List[StockCandidate] = []
        for c in candidates:
            if c.grade not in self.params["grade_target"]:
                continue
            if not self._is_defensive_sector(c.sector):
                continue
            filtered.append(c)

        self.log.info("scan_complete", strategy="B4", matched=len(filtered))
        return filtered

    # ── Signal generation ─────────────────────────────────────────────────

    async def generate_signal(
        self,
        stock: StockCandidate,
        market_data: Dict[str, Any],
    ) -> Optional[TradeSignal]:
        indicators: Dict[str, Any] = market_data.get("indicators", {})
        minute_candles = market_data.get("minute_candles")
        current_price: float = market_data["current_price"]
        rsi: float = indicators.get("rsi_14", 50)
        bb_lower: float = indicators.get("bb_lower", 0)

        # 1. Defensive sector reconfirmation
        if not self._is_defensive_sector(stock.sector):
            return None

        # 2. RSI extreme oversold
        if rsi >= self.params["rsi_threshold"]:
            return None

        # 3. BB lower band touch
        if not self._is_bb_lower_touch(
            current_price, bb_lower, self.params["bb_lower_touch_pct"]
        ):
            return None

        # 4. Volume spike reversal candle
        avg_minute_vol = (
            stock.avg_volume_20d / 390
            if stock.avg_volume_20d > 0
            else 0
        )
        if not self._detect_reversal_candle(
            minute_candles, avg_minute_vol, self.params["volume_spike_ratio"]
        ):
            return None

        stop_loss = current_price * (1 - self.params["stop_loss_pct"] / 100)

        self.log.info(
            "signal_generated",
            stock=stock.stock_code,
            sector=stock.sector,
            rsi=round(rsi, 1),
            bb_lower=round(bb_lower),
        )

        return TradeSignal(
            stock_code=stock.stock_code,
            action="BUY",
            strategy_code="B4",
            entry_price=current_price,
            stop_loss=stop_loss,
            target_prices=[
                current_price * (1 + self.params["target_pct"] / 100),
            ],
            position_pct=self._adjust_position(stock.confidence, stock.grade),
            confidence=min(stock.confidence, 3),
            reason=(
                f"과매도 역발상: RSI {rsi:.1f}, "
                f"BB 하단 {bb_lower:,.0f}원 터치, "
                f"방어섹터({stock.sector}), 거래량 반전"
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
            "max_holding_hours": self.params["max_holding_days"] * 6,  # ~6 trading hrs/day
        }
