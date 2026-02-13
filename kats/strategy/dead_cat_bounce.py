"""
KATS Dead Cat Bounce Strategy (B1)

Bear-market counter-trend strategy capturing short-term technical rebounds
in large-cap stocks after sharp declines.

Core logic:
    1. Stock dropped 5-10 % from previous close (panic selling).
    2. RSI(14) < 30 (oversold extreme).
    3. Volume reversal: current volume spike with bullish candle.
    4. Target +4 % (quick scalp); stop -2 %; maximum 4-hour hold.

Entry: RSI oversold + volume reversal candle.
Exit: +4 % target OR -2 % stop OR 4h time limit.
Focus: A grade large-cap only (most liquid, least manipulation risk).
Position: 12.5 % base.

References:
    - Kathryn Staley, "The Art of Short Selling" (pattern recognition)
    - Mark Turner, bear-market bounce techniques
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


class DeadCatBounceStrategy(BaseStrategy):
    """Dead cat bounce -- bear-market quick-scalp on oversold rebounds.

    Trades only A-grade large-caps to minimise manipulation risk.
    Strictly time-limited to 4 hours maximum holding.

    Category: BEAR (BEAR, STRONG_BEAR regimes).
    """

    def __init__(self) -> None:
        super().__init__("B1", StrategyCategory.BEAR)
        self.params: Dict[str, Any] = {
            "min_drop_pct": 5,
            "max_drop_pct": 10,
            "rsi_threshold": 30,
            "volume_spike_ratio": 1.5,
            "target_pct": 4,
            "stop_loss_pct": 2,
            "max_holding_hours": 4,
            "grade_target": ["A"],
            "position_pct": 12.5,
        }

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _calculate_drop_pct(prev_close: float, current_price: float) -> float:
        """Return the absolute percentage drop from previous close."""
        if prev_close <= 0:
            return 0.0
        return (prev_close - current_price) / prev_close * 100

    @staticmethod
    def _detect_volume_reversal(
        minute_candles: Any,
        avg_minute_volume: float,
        spike_ratio: float,
    ) -> bool:
        """Detect a bullish volume reversal candle.

        Conditions:
            * Last candle is bullish (close > open).
            * Volume is >= spike_ratio * average minute volume.
        """
        try:
            opens = minute_candles["open"].values
            closes = minute_candles["close"].values
            volumes = minute_candles["volume"].values
        except (KeyError, AttributeError):
            return False

        if len(opens) < 1:
            return False

        last_bullish = closes[-1] > opens[-1]
        last_vol = float(volumes[-1])
        vol_spike = last_vol >= avg_minute_volume * spike_ratio

        return last_bullish and vol_spike

    # ── Scan ──────────────────────────────────────────────────────────────

    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Filter to A-grade large-cap stocks only."""
        min_market_cap = 1_000_000_000_000  # 1 조 원
        filtered: List[StockCandidate] = []
        for c in candidates:
            if c.grade not in self.params["grade_target"]:
                continue
            if c.market_cap < min_market_cap:
                continue
            filtered.append(c)

        self.log.info("scan_complete", strategy="B1", matched=len(filtered))
        return filtered

    # ── Signal generation ─────────────────────────────────────────────────

    async def generate_signal(
        self,
        stock: StockCandidate,
        market_data: Dict[str, Any],
    ) -> Optional[TradeSignal]:
        indicators: Dict[str, Any] = market_data.get("indicators", {})
        prev_close: float = market_data["prev_day"]["close"]
        current_price: float = market_data["current_price"]
        minute_candles = market_data.get("minute_candles")
        rsi: float = indicators.get("rsi_14", 50)

        # 1. Drop magnitude check
        drop_pct = self._calculate_drop_pct(prev_close, current_price)
        if not (self.params["min_drop_pct"] <= drop_pct <= self.params["max_drop_pct"]):
            return None

        # 2. RSI oversold check
        if rsi >= self.params["rsi_threshold"]:
            return None

        # 3. Volume reversal confirmation
        avg_minute_vol = (
            stock.avg_volume_20d / 390
            if stock.avg_volume_20d > 0
            else 0
        )
        if not self._detect_volume_reversal(
            minute_candles, avg_minute_vol, self.params["volume_spike_ratio"]
        ):
            return None

        stop_loss = current_price * (1 - self.params["stop_loss_pct"] / 100)

        self.log.info(
            "signal_generated",
            stock=stock.stock_code,
            drop_pct=round(drop_pct, 2),
            rsi=round(rsi, 1),
        )

        return TradeSignal(
            stock_code=stock.stock_code,
            action="BUY",
            strategy_code="B1",
            entry_price=current_price,
            stop_loss=stop_loss,
            target_prices=[
                current_price * (1 + self.params["target_pct"] / 100),
            ],
            position_pct=self._adjust_position(stock.confidence, stock.grade),
            confidence=min(stock.confidence, 3),
            reason=(
                f"데드캣 바운스: {drop_pct:.1f}% 급락, "
                f"RSI {rsi:.1f} 과매도, "
                f"거래량 반전 확인 (최대 {self.params['max_holding_hours']}시간)"
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
            "max_holding_hours": self.params["max_holding_hours"],
        }
