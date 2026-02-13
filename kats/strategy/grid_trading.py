"""
KATS Grid Trading Strategy (GR)

Automated grid trading for sideways / range-bound markets.

Core logic:
    * Divide a price band into N equally-spaced levels.
    * Place BUY orders at levels below the center and SELL orders above.
    * Each grid level uses a fixed percentage of capital.
    * Halt the strategy if price exits the grid boundaries.

Suitable for KOSPI blue-chips with low volatility periods.
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


class GridTradingStrategy(BaseStrategy):
    """Mechanical grid trading strategy.

    Parameters:
        grid_count: Number of grid levels (default 10).
        grid_range_pct: Total range as percentage of center price (default 10 %).
        order_size_pct: Capital allocation per grid order (default 5 %).
        max_position_pct: Maximum cumulative position (default 30 %).

    Category: NEUTRAL -- operates in any market regime, best in SIDEWAYS.
    """

    def __init__(self) -> None:
        super().__init__("GR", StrategyCategory.NEUTRAL)
        self.params: Dict[str, Any] = {
            "grid_count": 10,
            "grid_range_pct": 10,
            "order_size_pct": 5,
            "max_position_pct": 30,
            "grade_target": ["A", "B", "ETF"],
            "position_pct": 5.0,
        }
        # Active grid state per stock (populated by calculate_grid)
        self._active_grids: Dict[str, List[Dict[str, Any]]] = {}

    # ── Grid calculation ──────────────────────────────────────────────────

    def calculate_grid(self, center_price: float) -> List[Dict[str, Any]]:
        """Compute evenly-spaced grid levels around *center_price*.

        Returns:
            List of grid-level dicts with keys:
                ``level``, ``price``, ``action``, ``filled``.
            Levels below center are BUY; levels at or above center are SELL.
        """
        half_range = center_price * self.params["grid_range_pct"] / 100 / 2
        grid_step = (half_range * 2) / self.params["grid_count"]

        grids: List[Dict[str, Any]] = []
        for i in range(self.params["grid_count"] + 1):
            price = center_price - half_range + (grid_step * i)
            action = "BUY" if price < center_price else "SELL"
            grids.append(
                {
                    "level": i,
                    "price": round(price),
                    "action": action,
                    "filled": False,
                }
            )
        self.log.info(
            "grid_calculated",
            center=center_price,
            levels=len(grids),
            low=grids[0]["price"],
            high=grids[-1]["price"],
        )
        return grids

    # ── Scan ──────────────────────────────────────────────────────────────

    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Filter to large-cap stocks suitable for range trading.

        Selection criteria:
            * Grade in target list.
            * Trend score <= 60 (avoid strong directional moves).
            * Price within 25 % of 52-week midpoint (range-bound proxy).
        """
        filtered: List[StockCandidate] = []
        for c in candidates:
            if c.grade not in self.params["grade_target"]:
                continue
            # Prefer range-bound stocks
            if c.trend_score > 60:
                continue
            midpoint = (c.week52_high + c.week52_low) / 2
            if midpoint <= 0:
                continue
            distance_pct = abs(c.price - midpoint) / midpoint * 100
            if distance_pct > 25:
                continue
            filtered.append(c)

        self.log.info("scan_complete", strategy="GR", matched=len(filtered))
        return filtered

    # ── Signal generation ─────────────────────────────────────────────────

    async def generate_signal(
        self,
        stock: StockCandidate,
        market_data: Dict[str, Any],
    ) -> Optional[TradeSignal]:
        current_price: float = market_data["current_price"]
        indicators: Dict[str, Any] = market_data.get("indicators", {})
        code = stock.stock_code

        # Initialise grid if not set for this stock
        if code not in self._active_grids:
            self._active_grids[code] = self.calculate_grid(current_price)

        grids = self._active_grids[code]

        # Check if price has left the grid entirely
        if current_price < grids[0]["price"] or current_price > grids[-1]["price"]:
            self.log.warning(
                "grid_breached",
                stock=code,
                price=current_price,
                grid_low=grids[0]["price"],
                grid_high=grids[-1]["price"],
            )
            # Remove grid -- strategy paused for this stock
            del self._active_grids[code]
            return None

        # Find the nearest unfilled grid level
        nearest_level: Optional[Dict[str, Any]] = None
        min_distance = float("inf")
        for level in grids:
            if level["filled"]:
                continue
            dist = abs(current_price - level["price"])
            # Only trigger if price is within 0.3 % of a grid line
            if dist / level["price"] * 100 <= 0.3 and dist < min_distance:
                min_distance = dist
                nearest_level = level

        if nearest_level is None:
            return None

        # Enforce max position
        filled_count = sum(1 for g in grids if g["filled"] and g["action"] == "BUY")
        cumulative_pct = filled_count * self.params["order_size_pct"]
        if (
            nearest_level["action"] == "BUY"
            and cumulative_pct >= self.params["max_position_pct"]
        ):
            self.log.debug("max_grid_position_reached", stock=code)
            return None

        # Mark as filled
        nearest_level["filled"] = True

        # Stop loss = grid low boundary
        stop_loss = grids[0]["price"] * 0.98

        # Target prices = next grid levels in opposite direction
        target_prices = [
            g["price"]
            for g in grids
            if not g["filled"]
            and g["action"] != nearest_level["action"]
        ][:3] or [current_price * 1.03]

        self.log.info(
            "grid_signal",
            stock=code,
            action=nearest_level["action"],
            level=nearest_level["level"],
            price=nearest_level["price"],
        )

        return TradeSignal(
            stock_code=code,
            action=nearest_level["action"],
            strategy_code="GR",
            entry_price=current_price,
            stop_loss=stop_loss,
            target_prices=target_prices,
            position_pct=self.params["order_size_pct"],
            confidence=min(stock.confidence, 3),
            reason=(
                f"그리드 매매: 레벨 {nearest_level['level']} "
                f"({nearest_level['action']}) "
                f"가격 {nearest_level['price']:,.0f}원 도달"
            ),
            indicators_snapshot=self._capture_snapshot(indicators),
        )

    # ── Exit rules ────────────────────────────────────────────────────────

    def get_exit_rules(self) -> Dict[str, Any]:
        return {
            "stop_loss_pct": 2.0,
            "target_prices_pct": [],
            "trailing_stop": False,
            "trailing_stop_pct": None,
            "time_exit": None,
            "max_holding_hours": None,
        }

    # ── Utilities ─────────────────────────────────────────────────────────

    def reset_grid(self, stock_code: str) -> None:
        """Remove the active grid for a stock so it can be recalculated."""
        self._active_grids.pop(stock_code, None)
        self.log.info("grid_reset", stock=stock_code)
