"""
KATS Grade Allocator -- Regime-Based Capital Allocation

Determines how much of total capital can be allocated to each stock grade
under the current market regime.  Enforces:
    1. Per-grade total position limits
    2. Per-sector concentration limits (40%)
    3. Minimum cash reserve requirements

Allocation tables are calibrated per-regime to shift capital towards
safer grades in bear markets and allow more aggressive allocation in
bull markets.

References:
    - Minervini, tiered stock selection by quality
    - O'Neil, always maintain a cash buffer
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import structlog

from kats.config.constants import StockGrade
from kats.strategy.base_strategy import MarketRegime

logger = structlog.get_logger(__name__)


# ── Regime Allocation Table ────────────────────────────────────────────
# Values are percentages of total capital.
# grade_a_pct + grade_b_pct + grade_c_pct + cash_pct = 100%

@dataclass(frozen=True)
class RegimeAllocation:
    """Capital allocation split for a given regime."""
    grade_a_pct: float
    grade_b_pct: float
    grade_c_pct: float
    cash_pct: float

    def __post_init__(self) -> None:
        total = self.grade_a_pct + self.grade_b_pct + self.grade_c_pct + self.cash_pct
        if abs(total - 100.0) > 0.01:
            raise ValueError(
                f"Allocation must sum to 100%, got {total:.2f}%"
            )


REGIME_ALLOCATION: Dict[MarketRegime, RegimeAllocation] = {
    MarketRegime.STRONG_BULL: RegimeAllocation(
        grade_a_pct=40.0,
        grade_b_pct=30.0,
        grade_c_pct=10.0,
        cash_pct=20.0,
    ),
    MarketRegime.BULL: RegimeAllocation(
        grade_a_pct=35.0,
        grade_b_pct=25.0,
        grade_c_pct=10.0,
        cash_pct=30.0,
    ),
    MarketRegime.SIDEWAYS: RegimeAllocation(
        grade_a_pct=25.0,
        grade_b_pct=15.0,
        grade_c_pct=5.0,
        cash_pct=55.0,
    ),
    MarketRegime.BEAR: RegimeAllocation(
        grade_a_pct=15.0,
        grade_b_pct=10.0,
        grade_c_pct=0.0,
        cash_pct=75.0,
    ),
    MarketRegime.STRONG_BEAR: RegimeAllocation(
        grade_a_pct=10.0,
        grade_b_pct=0.0,
        grade_c_pct=0.0,
        cash_pct=90.0,
    ),
}

# Per-sector concentration cap
SECTOR_MAX_PCT = 40.0


class GradeAllocator:
    """
    Validates whether a new position conforms to the regime-based
    allocation plan and concentration limits.

    Usage::

        allocator = GradeAllocator()
        ok, reason = allocator.validate_allocation(
            signal=signal_dict,
            current_positions=positions_list,
            regime=MarketRegime.BULL,
        )
    """

    def __init__(
        self,
        regime_allocation: Dict[MarketRegime, RegimeAllocation] | None = None,
        sector_max_pct: float = SECTOR_MAX_PCT,
    ) -> None:
        self._allocation = regime_allocation or REGIME_ALLOCATION
        self._sector_max_pct = sector_max_pct

    # ── Core API ───────────────────────────────────────────────────────

    def validate_allocation(
        self,
        signal: Dict[str, Any],
        current_positions: List[Dict[str, Any]],
        regime: MarketRegime,
    ) -> Tuple[bool, str]:
        """
        Validate whether the proposed signal fits within allocation limits.

        Args:
            signal: Must contain at minimum:
                ``stock_code``, ``grade`` (str: A/B/C), ``position_pct`` (float),
                ``sector`` (str).
            current_positions: List of position dicts, each containing:
                ``stock_code``, ``grade``, ``position_pct``, ``sector``.
            regime: Current market regime.

        Returns:
            (passed: bool, reason: str)
            reason is empty string when passed=True.
        """
        grade_str = signal.get("grade", "")
        position_pct = signal.get("position_pct", 0.0)
        sector = signal.get("sector", "UNKNOWN")
        stock_code = signal.get("stock_code", "")

        log = logger.bind(
            stock_code=stock_code,
            grade=grade_str,
            position_pct=position_pct,
            sector=sector,
            regime=regime.value,
        )

        alloc = self._allocation.get(regime)
        if alloc is None:
            reason = f"No allocation table for regime {regime.value}"
            log.error("grade_allocator_no_regime", reason=reason)
            return False, reason

        # ── 1. Grade total limit check ─────────────────────────────────
        grade_limit_pct = self._grade_limit(alloc, grade_str)
        current_grade_pct = self._sum_grade_pct(current_positions, grade_str)
        projected = current_grade_pct + position_pct

        if projected > grade_limit_pct:
            reason = (
                f"Grade {grade_str} allocation would reach {projected:.1f}% "
                f"(limit {grade_limit_pct:.1f}% for {regime.value}). "
                f"Current: {current_grade_pct:.1f}%, requested: {position_pct:.1f}%."
            )
            log.warning("grade_allocator_grade_limit", reason=reason)
            return False, reason

        # ── 2. Sector concentration check (40%) ───────────────────────
        current_sector_pct = self._sum_sector_pct(current_positions, sector)
        projected_sector = current_sector_pct + position_pct

        if projected_sector > self._sector_max_pct:
            reason = (
                f"Sector '{sector}' would reach {projected_sector:.1f}% "
                f"(limit {self._sector_max_pct:.1f}%). "
                f"Current: {current_sector_pct:.1f}%, requested: {position_pct:.1f}%."
            )
            log.warning("grade_allocator_sector_limit", reason=reason)
            return False, reason

        # ── 3. Minimum cash requirement ────────────────────────────────
        total_invested_pct = self._sum_all_pct(current_positions) + position_pct
        projected_cash_pct = 100.0 - total_invested_pct
        min_cash_pct = alloc.cash_pct

        if projected_cash_pct < min_cash_pct:
            reason = (
                f"Cash reserve would drop to {projected_cash_pct:.1f}% "
                f"(minimum {min_cash_pct:.1f}% for {regime.value}). "
                f"Total invested: {total_invested_pct:.1f}%."
            )
            log.warning("grade_allocator_cash_limit", reason=reason)
            return False, reason

        log.info("grade_allocator_passed")
        return True, ""

    # ── Query helpers ──────────────────────────────────────────────────

    def get_regime_allocation(self, regime: MarketRegime) -> Dict[str, float]:
        """Return the allocation table for the given regime as a dict."""
        alloc = self._allocation.get(regime)
        if alloc is None:
            return {}
        return {
            "grade_a_pct": alloc.grade_a_pct,
            "grade_b_pct": alloc.grade_b_pct,
            "grade_c_pct": alloc.grade_c_pct,
            "cash_pct": alloc.cash_pct,
        }

    def get_remaining_capacity(
        self,
        grade: str,
        regime: MarketRegime,
        current_positions: List[Dict[str, Any]],
    ) -> float:
        """Return how much % capacity remains for the given grade."""
        alloc = self._allocation.get(regime)
        if alloc is None:
            return 0.0
        limit = self._grade_limit(alloc, grade)
        used = self._sum_grade_pct(current_positions, grade)
        return max(0.0, limit - used)

    # ── Internal helpers ───────────────────────────────────────────────

    @staticmethod
    def _grade_limit(alloc: RegimeAllocation, grade: str) -> float:
        return {
            "A": alloc.grade_a_pct,
            StockGrade.A: alloc.grade_a_pct,
            "B": alloc.grade_b_pct,
            StockGrade.B: alloc.grade_b_pct,
            "C": alloc.grade_c_pct,
            StockGrade.C: alloc.grade_c_pct,
        }.get(grade, 0.0)

    @staticmethod
    def _sum_grade_pct(
        positions: List[Dict[str, Any]], grade: str,
    ) -> float:
        return sum(
            p.get("position_pct", 0.0)
            for p in positions
            if p.get("grade") == grade
        )

    @staticmethod
    def _sum_sector_pct(
        positions: List[Dict[str, Any]], sector: str,
    ) -> float:
        return sum(
            p.get("position_pct", 0.0)
            for p in positions
            if p.get("sector") == sector
        )

    @staticmethod
    def _sum_all_pct(positions: List[Dict[str, Any]]) -> float:
        return sum(p.get("position_pct", 0.0) for p in positions)

    def __repr__(self) -> str:
        return f"GradeAllocator(sector_max={self._sector_max_pct}%)"
