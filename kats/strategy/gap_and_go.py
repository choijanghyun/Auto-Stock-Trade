"""
KATS Gap & Go Strategy (S2)

Intraday momentum strategy that capitalises on opening gaps with follow-through.

Core logic:
    1. Detect a gap-up of 2-5 % vs previous close.
    2. Wait for a pullback toward VWAP or intraday support.
    3. Confirm volume pickup on the bounce (volume > 1.3x average minute vol).
    4. Enter long with tight stop below the pullback low.

Entry: Pullback-to-support bounce with volume confirmation.
Exit: Quick scalp -- target +3 %, +5 %; stop -2 %.
Focus: B-C grade stocks with high intraday turnover, 12.5 % base position.

References:
    - Andrew Aziz, "How to Day Trade for a Living"
    - Cameron, gap-and-go morning setups
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


class GapAndGoStrategy(BaseStrategy):
    """Gap & Go intraday pullback strategy.

    Looks for 2-5 % opening gaps, waits for a pullback to VWAP or the
    first-5-minute support, then enters on a bounce candle with volume.

    Category: BULL (STRONG_BULL, BULL regimes).
    """

    def __init__(self) -> None:
        super().__init__("S2", StrategyCategory.BULL)
        self.params: Dict[str, Any] = {
            "gap_min_pct": 2.0,
            "gap_max_pct": 5.0,
            "pullback_vwap_tolerance_pct": 0.5,
            "volume_confirmation_ratio": 1.3,
            "stop_loss_pct": 2.0,
            "target_1_pct": 3.0,
            "target_2_pct": 5.0,
            "grade_target": ["B", "C"],
            "position_pct": 12.5,
        }

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _calculate_gap_pct(prev_close: float, today_open: float) -> float:
        """Return the gap percentage from previous close to today's open."""
        if prev_close <= 0:
            return 0.0
        return (today_open - prev_close) / prev_close * 100

    @staticmethod
    def _detect_pullback_to_support(
        minute_candles: Any,
        vwap: float,
        tolerance_pct: float,
    ) -> bool:
        """Check whether recent price action pulled back close to VWAP.

        A pullback is confirmed when the low of any of the last 5 candles
        came within *tolerance_pct* of VWAP.
        """
        try:
            lows = minute_candles["low"].values[-5:]
        except (KeyError, AttributeError, IndexError):
            return False

        if vwap <= 0:
            return False

        for low in lows:
            dist = (low - vwap) / vwap * 100
            if -tolerance_pct <= dist <= tolerance_pct:
                return True
        return False

    @staticmethod
    def _confirm_bounce_volume(
        minute_candles: Any,
        avg_minute_volume: float,
        ratio: float,
    ) -> bool:
        """Confirm that the most recent candle's volume exceeds *ratio*
        times the average minute volume.
        """
        try:
            last_vol = float(minute_candles["volume"].values[-1])
        except (KeyError, AttributeError, IndexError):
            return False
        return last_vol >= avg_minute_volume * ratio

    # ── Scan ──────────────────────────────────────────────────────────────

    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Pre-filter to liquid, lower-grade stocks that tend to gap."""
        min_turnover = 500_000_000  # 5 억 원
        filtered: List[StockCandidate] = []
        for c in candidates:
            if c.grade not in self.params["grade_target"]:
                continue
            if c.avg_turnover_20d < min_turnover:
                continue
            # Prefer stocks with higher recent volatility
            if c.week52_high > 0:
                price_range_pct = (
                    (c.week52_high - c.week52_low) / c.week52_high * 100
                )
                if price_range_pct < 20:
                    continue
            filtered.append(c)

        self.log.info("scan_complete", strategy="S2", matched=len(filtered))
        return filtered

    # ── Signal generation ─────────────────────────────────────────────────

    async def generate_signal(
        self,
        stock: StockCandidate,
        market_data: Dict[str, Any],
    ) -> Optional[TradeSignal]:
        indicators: Dict[str, Any] = market_data.get("indicators", {})
        prev_close: float = market_data["prev_day"]["close"]
        today_open: float = market_data["today_open"]
        current_price: float = market_data["current_price"]
        minute_candles = market_data.get("minute_candles")
        vwap: float = indicators.get("vwap", 0)

        # 1. Gap size validation
        gap_pct = self._calculate_gap_pct(prev_close, today_open)
        if not (self.params["gap_min_pct"] <= gap_pct <= self.params["gap_max_pct"]):
            return None

        # 2. Pullback to VWAP / support
        if not self._detect_pullback_to_support(
            minute_candles, vwap, self.params["pullback_vwap_tolerance_pct"]
        ):
            return None

        # 3. Volume confirmation on bounce
        avg_minute_vol = (
            stock.avg_volume_20d / 390  # ~390 trading minutes per day
            if stock.avg_volume_20d > 0
            else 0
        )
        if not self._confirm_bounce_volume(
            minute_candles, avg_minute_vol, self.params["volume_confirmation_ratio"]
        ):
            return None

        # 4. Price must still be above VWAP (momentum intact)
        if current_price < vwap:
            return None

        stop_loss = current_price * (1 - self.params["stop_loss_pct"] / 100)

        # Pullback low for tighter stop
        try:
            recent_low = float(np.min(minute_candles["low"].values[-5:]))
            stop_loss = max(stop_loss, recent_low * 0.998)
        except (KeyError, AttributeError, IndexError, ValueError):
            pass

        self.log.info(
            "signal_generated",
            stock=stock.stock_code,
            gap_pct=round(gap_pct, 2),
            vwap=round(vwap),
        )

        return TradeSignal(
            stock_code=stock.stock_code,
            action="BUY",
            strategy_code="S2",
            entry_price=current_price,
            stop_loss=stop_loss,
            target_prices=[
                current_price * (1 + self.params["target_1_pct"] / 100),
                current_price * (1 + self.params["target_2_pct"] / 100),
            ],
            position_pct=self._adjust_position(stock.confidence, stock.grade),
            confidence=stock.confidence,
            reason=(
                f"Gap & Go: 갭 {gap_pct:.1f}%, VWAP {vwap:,.0f}원 눌림 후 반등, "
                f"거래량 확인"
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
            "trailing_stop": False,
            "trailing_stop_pct": None,
            "time_exit": "MARKET_CLOSE",
            "max_holding_hours": 6,
        }
