"""
KATS SEPA Momentum Breakout Strategy (S1)

Mark Minervini's Specific Entry Point Analysis (SEPA) adapted for KOSPI.

Core logic:
    1. Detect Volatility Contraction Pattern (VCP) -- 3+ contractions.
    2. Identify the pivot point (resistance of the tightest contraction).
    3. Confirm breakout above pivot with volume >= 1.5x 20-day average.
    4. Validate fundamentals: EPS growth >= 20 %, Revenue growth >= 15 %.

Entry: Breakout above pivot with volume confirmation.
Exit: Stop -7 % below pivot; targets +10 %, +20 %, trailing.
Focus: B grade mid-cap stocks, 17.5 % base position.

References:
    - Mark Minervini, "Trade Like a Stock Market Wizard"
    - Mark Minervini, "Think & Trade Like a Champion"
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


class SEPAMomentumStrategy(BaseStrategy):
    """Minervini SEPA momentum breakout -- trend-following strategy.

    Scans for VCP (Volatility Contraction Pattern), then enters on a
    pivot breakout with strong volume and confirmed earnings growth.

    Category: BULL (STRONG_BULL, BULL regimes).
    """

    def __init__(self) -> None:
        super().__init__("S1", StrategyCategory.BULL)
        self.params: Dict[str, Any] = {
            "vcp_contractions": 3,
            "volume_breakout_ratio": 1.5,
            "eps_min_growth": 20,
            "revenue_min_growth": 15,
            "stop_loss_pct": 7,
            "target_1_pct": 10,
            "target_2_pct": 20,
            "grade_target": ["B"],
            "position_pct": 17.5,
        }

    # ── VCP detection ─────────────────────────────────────────────────────

    @staticmethod
    def _detect_vcp(
        daily_prices: Any,
        min_contractions: int = 3,
    ) -> bool:
        """Detect Volatility Contraction Pattern in *daily_prices*.

        A VCP is identified by successive swing ranges (high-low within a
        consolidation) that become progressively tighter.  We require at
        least *min_contractions* such contractions.

        Args:
            daily_prices: DataFrame with ``high`` and ``low`` columns,
                ordered chronologically (oldest first).
            min_contractions: Minimum number of tightening swings.

        Returns:
            ``True`` if a valid VCP is detected.
        """
        try:
            highs = daily_prices["high"].values
            lows = daily_prices["low"].values
        except (KeyError, AttributeError):
            return False

        if len(highs) < 40:
            return False

        # Analyse the last 60 bars (or available) for consolidation
        window = min(60, len(highs))
        highs = highs[-window:]
        lows = lows[-window:]

        # Split into segments and measure each segment's range
        segment_size = window // (min_contractions + 1)
        if segment_size < 5:
            return False

        ranges: List[float] = []
        for i in range(min_contractions + 1):
            start = i * segment_size
            end = start + segment_size
            seg_range = float(np.max(highs[start:end]) - np.min(lows[start:end]))
            ranges.append(seg_range)

        # Count how many successive ranges are tighter
        contractions = 0
        for i in range(1, len(ranges)):
            if ranges[i] < ranges[i - 1]:
                contractions += 1

        return contractions >= min_contractions

    @staticmethod
    def _calculate_pivot(daily_prices: Any) -> float:
        """Calculate the pivot (breakout) price from a VCP pattern.

        The pivot is the highest high in the most recent tight consolidation
        (last 10 bars).
        """
        try:
            recent_highs = daily_prices["high"].values[-10:]
        except (KeyError, AttributeError, IndexError):
            return 0.0
        return float(np.max(recent_highs))

    # ── Minervini Trend Template check ────────────────────────────────────

    @staticmethod
    def _passes_trend_template(stock: StockCandidate) -> bool:
        """Verify the Minervini Trend Template conditions:

        1. Price > MA50 > MA150 > MA200
        2. MA200 slope is positive (rising for >= 1 month)
        3. Price is >= 30 % above 52-week low
        4. Price is within 25 % of 52-week high
        5. RS rank >= 70
        """
        if not (stock.price > stock.ma_50 > stock.ma_150 > stock.ma_200):
            return False
        if stock.ma_200_slope <= 0:
            return False
        if stock.week52_low <= 0:
            return False
        above_low_pct = (stock.price - stock.week52_low) / stock.week52_low * 100
        if above_low_pct < 30:
            return False
        if stock.week52_high <= 0:
            return False
        below_high_pct = (stock.week52_high - stock.price) / stock.week52_high * 100
        if below_high_pct > 25:
            return False
        if stock.rs_rank < 70:
            return False
        return True

    # ── Scan ──────────────────────────────────────────────────────────────

    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Filter candidates through Minervini Trend Template + fundamental
        growth criteria.
        """
        filtered: List[StockCandidate] = []
        for c in candidates:
            if c.grade not in self.params["grade_target"]:
                continue
            if c.eps_growth_qoq < self.params["eps_min_growth"]:
                continue
            if c.revenue_growth < self.params["revenue_min_growth"]:
                continue
            if not self._passes_trend_template(c):
                continue
            filtered.append(c)

        self.log.info("scan_complete", strategy="S1", matched=len(filtered))
        return filtered

    # ── Signal generation ─────────────────────────────────────────────────

    async def generate_signal(
        self,
        stock: StockCandidate,
        market_data: Dict[str, Any],
    ) -> Optional[TradeSignal]:
        indicators: Dict[str, Any] = market_data.get("indicators", {})
        daily_prices = market_data.get("daily_prices")
        current_price: float = market_data["current_price"]
        current_volume: int = market_data.get("current_volume", 0)

        # 1. VCP pattern confirmation
        if not self._detect_vcp(
            daily_prices, self.params["vcp_contractions"]
        ):
            return None

        # 2. Pivot breakout
        pivot = self._calculate_pivot(daily_prices)
        if pivot <= 0 or current_price < pivot:
            return None

        # 3. Volume confirmation (>= 1.5x average)
        vol_ratio = (
            current_volume / stock.avg_volume_20d
            if stock.avg_volume_20d > 0
            else 0
        )
        if vol_ratio < self.params["volume_breakout_ratio"]:
            return None

        # 4. Fundamental re-check (belt & suspenders)
        if stock.eps_growth_qoq < self.params["eps_min_growth"]:
            return None
        if stock.revenue_growth < self.params["revenue_min_growth"]:
            return None

        stop_loss = pivot * (1 - self.params["stop_loss_pct"] / 100)

        self.log.info(
            "signal_generated",
            stock=stock.stock_code,
            pivot=round(pivot),
            vol_ratio=round(vol_ratio, 2),
        )

        return TradeSignal(
            stock_code=stock.stock_code,
            action="BUY",
            strategy_code="S1",
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
                f"SEPA 모멘텀 돌파: VCP 패턴 완성, "
                f"피벗 {pivot:,.0f}원 돌파, "
                f"거래량 {vol_ratio:.1f}배"
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
            "trailing_stop_pct": 5.0,
            "time_exit": None,
            "max_holding_hours": None,
        }
