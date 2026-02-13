"""
KATS CAN SLIM Breakout Strategy (S3)

William O'Neil's CAN SLIM methodology adapted for Korean equities.

CAN SLIM scoring:
    C - Current quarterly EPS growth (>= 25 %)
    A - Annual EPS growth over 5 years (>= 25 %)
    N - New products, management, or price highs (catalyst present)
    S - Supply & Demand (low float, volume surges)
    L - Leader or laggard (RS rank >= 80)
    I - Institutional sponsorship (net institutional buying)
    M - Market direction (bull regime confirmed)

Entry: CAN SLIM score >= 70 AND cup-with-handle / flat-base breakout.
Exit: Stop -8 % from pivot; targets +20 %, +30 %, trailing.
Focus: A grade blue-chips, 25 % base position.

References:
    - William O'Neil, "How to Make Money in Stocks"
    - IBD (Investor's Business Daily) methodology
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


class CANSLIMBreakoutStrategy(BaseStrategy):
    """O'Neil CAN SLIM breakout strategy.

    Combines fundamental quality scoring (CAN SLIM) with technical
    base-breakout patterns (cup-with-handle, flat base).

    Category: BULL (STRONG_BULL, BULL regimes).
    """

    def __init__(self) -> None:
        super().__init__("S3", StrategyCategory.BULL)
        self.params: Dict[str, Any] = {
            # CAN SLIM thresholds
            "eps_qoq_min": 25,
            "eps_annual_min": 25,
            "rs_rank_min": 80,
            "canslim_score_min": 70,
            # Technical breakout
            "base_length_min_days": 20,
            "base_length_max_days": 65,
            "volume_breakout_ratio": 1.4,
            # Risk
            "stop_loss_pct": 8,
            "target_1_pct": 20,
            "target_2_pct": 30,
            "grade_target": ["A"],
            "position_pct": 25.0,
        }

    # ── CAN SLIM scoring ─────────────────────────────────────────────────

    def _score_canslim(
        self, stock: StockCandidate, indicators: Dict[str, Any]
    ) -> int:
        """Compute a 0-100 CAN SLIM score for *stock*.

        Each of the 7 factors contributes up to ~14 points.
        """
        score = 0

        # C - Current quarterly EPS
        if stock.eps_growth_qoq >= self.params["eps_qoq_min"]:
            score += 15
        elif stock.eps_growth_qoq >= self.params["eps_qoq_min"] * 0.6:
            score += 8

        # A - Annual EPS growth (use revenue_growth as proxy if annual EPS
        #     is unavailable at candidate level)
        if stock.revenue_growth >= self.params["eps_annual_min"]:
            score += 15
        elif stock.revenue_growth >= self.params["eps_annual_min"] * 0.6:
            score += 8

        # N - New highs / catalyst (price within 15 % of 52-week high)
        if stock.week52_high > 0:
            pct_from_high = (
                (stock.week52_high - stock.price) / stock.week52_high * 100
            )
            if pct_from_high <= 5:
                score += 15
            elif pct_from_high <= 15:
                score += 10

        # S - Supply & Demand (institutional + foreign net flow positive)
        if stock.inst_foreign_flow > 0:
            score += 15
        elif stock.inst_foreign_flow > -stock.avg_turnover_20d * 0.01:
            score += 7

        # L - Leader (RS rank)
        if stock.rs_rank >= self.params["rs_rank_min"]:
            score += 15
        elif stock.rs_rank >= 70:
            score += 8

        # I - Institutional sponsorship (positive flow as proxy)
        if stock.inst_foreign_flow > stock.avg_turnover_20d * 0.05:
            score += 15
        elif stock.inst_foreign_flow > 0:
            score += 8

        # M - Market direction (use trend_score as proxy; high = bullish)
        if stock.trend_score >= 70:
            score += 10
        elif stock.trend_score >= 50:
            score += 5

        return min(score, 100)

    # ── Base pattern detection ────────────────────────────────────────────

    def _detect_base_breakout(
        self, daily_prices: Any
    ) -> Optional[float]:
        """Detect a flat-base or cup-with-handle breakout.

        Returns the pivot price if a valid base is found, else ``None``.

        A simplified base is identified by:
            * A consolidation of 20-65 days where the price range is <= 15 %
              of the base high.
            * The pivot is the highest high within the base.
        """
        try:
            highs = daily_prices["high"].values
            lows = daily_prices["low"].values
            closes = daily_prices["close"].values
        except (KeyError, AttributeError):
            return None

        min_days = self.params["base_length_min_days"]
        max_days = self.params["base_length_max_days"]

        if len(highs) < max_days:
            return None

        # Scan backwards for the most recent valid base
        for length in range(max_days, min_days - 1, -5):
            base_highs = highs[-length:]
            base_lows = lows[-length:]
            base_high = float(np.max(base_highs))
            base_low = float(np.min(base_lows))

            if base_high <= 0:
                continue
            base_depth = (base_high - base_low) / base_high * 100
            if base_depth <= 15:
                # Valid flat base -- pivot is the base high
                return base_high

        # Cup-with-handle heuristic: look for a U-shape followed by
        # a smaller consolidation
        cup_len = 40
        if len(closes) >= cup_len + 10:
            cup = closes[-(cup_len + 10) : -10]
            handle = closes[-10:]
            cup_mid = cup[len(cup) // 2]
            cup_start = cup[0]
            cup_end = cup[-1]

            # Cup shape: middle lower than edges
            if cup_mid < cup_start * 0.95 and cup_end >= cup_start * 0.97:
                handle_high = float(np.max(handle))
                handle_low = float(np.min(handle))
                if handle_high > 0:
                    handle_depth = (handle_high - handle_low) / handle_high * 100
                    if handle_depth <= 10:
                        return handle_high

        return None

    # ── Scan ──────────────────────────────────────────────────────────────

    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Pre-filter to A-grade stocks with strong CAN SLIM fundamentals."""
        filtered: List[StockCandidate] = []
        for c in candidates:
            if c.grade not in self.params["grade_target"]:
                continue
            if c.eps_growth_qoq < self.params["eps_qoq_min"] * 0.5:
                continue
            if c.rs_rank < 60:
                continue
            if c.canslim_score >= self.params["canslim_score_min"] * 0.7:
                filtered.append(c)

        self.log.info("scan_complete", strategy="S3", matched=len(filtered))
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

        # 1. CAN SLIM score check
        score = self._score_canslim(stock, indicators)
        if score < self.params["canslim_score_min"]:
            return None

        # 2. Base breakout detection
        pivot = self._detect_base_breakout(daily_prices)
        if pivot is None or current_price < pivot:
            return None

        # 3. Volume on breakout
        vol_ratio = (
            current_volume / stock.avg_volume_20d
            if stock.avg_volume_20d > 0
            else 0
        )
        if vol_ratio < self.params["volume_breakout_ratio"]:
            return None

        stop_loss = pivot * (1 - self.params["stop_loss_pct"] / 100)

        self.log.info(
            "signal_generated",
            stock=stock.stock_code,
            canslim_score=score,
            pivot=round(pivot),
            vol_ratio=round(vol_ratio, 2),
        )

        return TradeSignal(
            stock_code=stock.stock_code,
            action="BUY",
            strategy_code="S3",
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
                f"CAN SLIM 돌파: 점수 {score}/100, "
                f"피벗 {pivot:,.0f}원 돌파, "
                f"거래량 {vol_ratio:.1f}배, "
                f"EPS +{stock.eps_growth_qoq:.0f}%"
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
