"""
KATS Volatility Breakout Strategy (VB)

Larry Williams' Volatility Breakout adapted for Korean equities.

Core logic:
    breakout_price = today_open + (prev_high - prev_low) * K
    If current_price >= breakout_price -> BUY
    Mandatory exit at market close (day-trade only).

References:
    - Larry Williams, "Long-Term Secrets to Short-Term Trading"
    - K factor default 0.5 (optimisable via backtest)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import structlog

from kats.strategy.base_strategy import (
    BaseStrategy,
    StockCandidate,
    StrategyCategory,
    TradeSignal,
)

logger = structlog.get_logger(__name__)


class VolatilityBreakoutStrategy(BaseStrategy):
    """Larry Williams Volatility Breakout -- day-trade strategy.

    Entry condition:
        ``current_price >= today_open + (prev_high - prev_low) * K``
    where K defaults to 0.5.

    Filters:
        * Previous day's range must be >= ``min_range_pct`` of the close
          to avoid low-volatility noise signals.

    Exit rules:
        * Mandatory liquidation at market close (15:20 KST).
        * Hard stop at ``today_open * 0.98`` (-2 % from open).

    Category: NEUTRAL (works in any regime).
    """

    def __init__(self) -> None:
        super().__init__("VB", StrategyCategory.NEUTRAL)
        self.params: Dict[str, Any] = {
            "k_factor": 0.5,
            "min_range_pct": 1.0,
            "stop_loss_pct": 2.0,
            "position_pct": 15.0,
            "grade_target": ["A", "B"],
        }

    # ── Scan ──────────────────────────────────────────────────────────────

    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Select candidates whose grade is in the target list and whose
        average daily turnover exceeds a minimum threshold for day-trading
        liquidity.
        """
        min_turnover = 1_000_000_000  # 10 억 원
        filtered: List[StockCandidate] = []
        for c in candidates:
            if c.grade not in self.params["grade_target"]:
                continue
            if c.avg_turnover_20d < min_turnover:
                continue
            filtered.append(c)
        self.log.info("scan_complete", strategy="VB", matched=len(filtered))
        return filtered

    # ── Signal generation ─────────────────────────────────────────────────

    async def generate_signal(
        self,
        stock: StockCandidate,
        market_data: Dict[str, Any],
    ) -> Optional[TradeSignal]:
        prev_day = market_data["prev_day"]
        prev_high: float = prev_day["high"]
        prev_low: float = prev_day["low"]
        prev_close: float = prev_day["close"]
        prev_range: float = prev_high - prev_low
        today_open: float = market_data["today_open"]
        current_price: float = market_data["current_price"]
        indicators: Dict[str, Any] = market_data.get("indicators", {})

        # Guard: previous range too small (noise)
        if prev_close <= 0:
            return None
        range_pct = (prev_range / prev_close) * 100
        if range_pct < self.params["min_range_pct"]:
            self.log.debug(
                "range_too_small",
                stock=stock.stock_code,
                range_pct=round(range_pct, 2),
            )
            return None

        # Calculate breakout threshold
        k = self.params["k_factor"]
        breakout_price = today_open + prev_range * k

        if current_price < breakout_price:
            return None

        stop_loss = today_open * (1 - self.params["stop_loss_pct"] / 100)

        self.log.info(
            "signal_generated",
            stock=stock.stock_code,
            breakout_price=round(breakout_price),
            current_price=current_price,
        )

        return TradeSignal(
            stock_code=stock.stock_code,
            action="BUY",
            strategy_code="VB",
            entry_price=current_price,
            stop_loss=stop_loss,
            target_prices=[0],  # 0 means exit at market close
            position_pct=self._adjust_position(stock.confidence, stock.grade),
            confidence=stock.confidence,
            reason=(
                f"변동성 돌파: 시가 {today_open:,.0f} + "
                f"전일변동폭 {prev_range:,.0f} x K({k}) = "
                f"목표가 {breakout_price:,.0f}원 돌파"
            ),
            indicators_snapshot=self._capture_snapshot(indicators),
        )

    # ── Exit rules ────────────────────────────────────────────────────────

    def get_exit_rules(self) -> Dict[str, Any]:
        return {
            "stop_loss_pct": self.params["stop_loss_pct"],
            "target_prices_pct": [],
            "trailing_stop": False,
            "trailing_stop_pct": None,
            "time_exit": "MARKET_CLOSE",
            "max_holding_hours": None,
        }
