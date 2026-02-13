"""
KATS VWAP Bounce Strategy (S5)

VWAP (Volume-Weighted Average Price) bounce strategy for institutional-grade
entry timing on strong stocks pulling back to VWAP support.

Core logic:
    1. Price is above VWAP (stock in institutional accumulation zone).
    2. Price pulls back within 0.5 % of VWAP (testing support).
    3. Two consecutive bounce candles confirm the VWAP hold.
    4. Enter long; stop below VWAP.

Entry: Bounce from VWAP with 2-candle confirmation.
Exit: Stop just below VWAP; targets +5 %, +10 %, trailing.
Focus: A grade large-cap stocks, 25 % base position.

References:
    - Brian Shannon, "Technical Analysis Using Multiple Timeframes"
    - John Carter, "Mastering the Trade"
    - Andrew Aziz, VWAP strategies
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


class VWAPBounceStrategy(BaseStrategy):
    """VWAP bounce institutional support strategy.

    Targets A-grade large-caps that are trading above VWAP and pull back
    to test it, then bounce with conviction.

    Category: BULL (STRONG_BULL, BULL regimes).
    """

    def __init__(self) -> None:
        super().__init__("S5", StrategyCategory.BULL)
        self.params: Dict[str, Any] = {
            "vwap_proximity_pct": 0.5,
            "bounce_candles": 2,
            "stop_loss_rule": "VWAP_BREAK",
            "stop_loss_buffer_pct": 0.5,
            "target_1_pct": 5.0,
            "target_2_pct": 10.0,
            "grade_target": ["A"],
            "position_pct": 25.0,
        }

    # ── Bounce confirmation ───────────────────────────────────────────────

    @staticmethod
    def _confirm_bounce(
        minute_candles: Any,
        vwap: float,
        required_candles: int = 2,
    ) -> bool:
        """Confirm that at least *required_candles* consecutive candles have
        bounced off VWAP (close > open, low near VWAP).

        A bounce candle:
            * close > open (bullish)
            * low is within 1 % of VWAP (tested VWAP support)
        """
        try:
            opens = minute_candles["open"].values
            closes = minute_candles["close"].values
            lows = minute_candles["low"].values
        except (KeyError, AttributeError):
            return False

        if len(opens) < required_candles:
            return False

        # Check the last N candles
        consecutive = 0
        for i in range(len(opens) - 1, max(len(opens) - required_candles - 3, -1), -1):
            is_bullish = closes[i] > opens[i]
            low_near_vwap = abs(lows[i] - vwap) / vwap * 100 <= 1.0 if vwap > 0 else False
            if is_bullish and low_near_vwap:
                consecutive += 1
            else:
                consecutive = 0
            if consecutive >= required_candles:
                return True

        return False

    # ── Scan ──────────────────────────────────────────────────────────────

    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Select A-grade large-caps with strong institutional interest."""
        min_turnover = 2_000_000_000  # 20 억 원
        filtered: List[StockCandidate] = []
        for c in candidates:
            if c.grade not in self.params["grade_target"]:
                continue
            if c.avg_turnover_20d < min_turnover:
                continue
            # Institutional flow must be net positive
            if c.inst_foreign_flow <= 0:
                continue
            filtered.append(c)

        self.log.info("scan_complete", strategy="S5", matched=len(filtered))
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
        vwap: float = indicators.get("vwap", 0)

        if vwap <= 0:
            return None

        # 1. Price must be above VWAP
        if current_price < vwap:
            return None

        # 2. Price within proximity of VWAP (pullback zone)
        distance_pct = (current_price - vwap) / vwap * 100
        if distance_pct > self.params["vwap_proximity_pct"]:
            return None

        # 3. Bounce candle confirmation
        if not self._confirm_bounce(
            minute_candles, vwap, self.params["bounce_candles"]
        ):
            return None

        # Stop just below VWAP
        stop_loss = vwap * (1 - self.params["stop_loss_buffer_pct"] / 100)

        self.log.info(
            "signal_generated",
            stock=stock.stock_code,
            vwap=round(vwap),
            distance_pct=round(distance_pct, 3),
        )

        return TradeSignal(
            stock_code=stock.stock_code,
            action="BUY",
            strategy_code="S5",
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
                f"VWAP 바운스: VWAP {vwap:,.0f}원 지지 확인, "
                f"거리 {distance_pct:.2f}%, 반등 캔들 확인"
            ),
            indicators_snapshot=self._capture_snapshot(indicators),
        )

    # ── Exit rules ────────────────────────────────────────────────────────

    def get_exit_rules(self) -> Dict[str, Any]:
        return {
            "stop_loss_pct": self.params["stop_loss_buffer_pct"],
            "target_prices_pct": [
                self.params["target_1_pct"],
                self.params["target_2_pct"],
            ],
            "trailing_stop": True,
            "trailing_stop_pct": 3.0,
            "time_exit": None,
            "max_holding_hours": None,
        }
