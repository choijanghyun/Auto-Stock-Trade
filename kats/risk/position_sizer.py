"""
KATS Position Sizer -- Van Tharp R-Multiple Based

Calculates position size based on:
    1. Market regime risk allocation (Van Tharp adaptive %)
    2. Stop-loss distance (R-multiple denominator)
    3. Grade-based maximum position cap
    4. Signal confidence multiplier

References:
    - Van Tharp, "Trade Your Way to Financial Freedom"
    - Minervini, "Trade Like a Stock Market Wizard" (risk per trade)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict

import structlog

from kats.config.constants import StockGrade
from kats.strategy.base_strategy import MarketRegime

logger = structlog.get_logger(__name__)

# ── Risk % by market regime (Van Tharp adaptive risk) ──────────────────
RISK_BY_REGIME: Dict[MarketRegime, float] = {
    MarketRegime.STRONG_BULL: 0.02,   # 2.0%
    MarketRegime.BULL: 0.018,         # 1.8%
    MarketRegime.SIDEWAYS: 0.012,     # 1.2%
    MarketRegime.BEAR: 0.008,         # 0.8%
    MarketRegime.STRONG_BEAR: 0.005,  # 0.5%
}

# ── Grade-based maximum single-position % of capital ───────────────────
GRADE_LIMIT: Dict[str, float] = {
    StockGrade.A: 0.30,  # 30%
    StockGrade.B: 0.20,  # 20%
    StockGrade.C: 0.10,  # 10%
}

# ── Confidence multiplier (5-star scale) ───────────────────────────────
CONFIDENCE_MULTIPLIER: Dict[int, float] = {
    5: 1.00,
    4: 0.75,
    3: 0.50,
    # confidence <= 2 => no trade
}

# Default R-multiple target for take-profit planning
DEFAULT_R_TARGET: float = 3.0


@dataclass(frozen=True)
class PositionSizeResult:
    """Immutable result of a position-sizing calculation."""

    position_amount: int        # KRW amount to allocate
    position_pct: float         # % of total capital
    quantity: int               # number of shares
    risk_amount_1r: int         # 1R in KRW (amount at risk)
    stop_loss_pct: float        # distance to stop as %
    r_multiple_target: float    # take-profit target in R
    regime_risk_pct: float      # base regime risk %
    grade_limit_pct: float      # grade cap %
    confidence_multiplier: float


class PositionSizer:
    """
    Van Tharp R-multiple position sizer.

    Usage::

        sizer = PositionSizer()
        result = sizer.calculate(
            total_capital=100_000_000,
            regime=MarketRegime.BULL,
            entry_price=50_000,
            stop_loss=47_500,
            grade=StockGrade.B,
            confidence=4,
        )
    """

    def __init__(
        self,
        risk_by_regime: Dict[MarketRegime, float] | None = None,
        grade_limit: Dict[str, float] | None = None,
        r_target: float = DEFAULT_R_TARGET,
    ) -> None:
        self._risk_by_regime = risk_by_regime or RISK_BY_REGIME
        self._grade_limit = grade_limit or GRADE_LIMIT
        self._r_target = r_target

    # ── public API ─────────────────────────────────────────────────────

    def calculate(
        self,
        total_capital: int,
        regime: MarketRegime,
        entry_price: int,
        stop_loss: int,
        grade: StockGrade,
        confidence: int,
    ) -> Dict[str, Any]:
        """
        Calculate position size and return a detail dict.

        Args:
            total_capital: Total account capital in KRW.
            regime: Current market regime.
            entry_price: Planned entry price per share (KRW).
            stop_loss: Stop-loss price per share (KRW).
            grade: Stock grade (A/B/C).
            confidence: Signal confidence score (1--5).

        Returns:
            dict with all sizing parameters, or a rejection dict when
            confidence is too low or inputs are invalid.
        """
        log = logger.bind(
            entry_price=entry_price,
            stop_loss=stop_loss,
            grade=grade.value,
            confidence=confidence,
            regime=regime.value,
        )

        # ── Guard: confidence too low ──────────────────────────────────
        if confidence <= 2:
            log.info("position_sizer_rejected", reason="confidence_too_low")
            return self._rejection("confidence_too_low", confidence=confidence)

        # ── Guard: stop-loss must be below entry for long positions ─────
        if stop_loss >= entry_price:
            log.warning(
                "position_sizer_rejected",
                reason="stop_loss_above_entry",
            )
            return self._rejection(
                "stop_loss_above_entry",
                entry_price=entry_price,
                stop_loss=stop_loss,
            )

        # ── Guard: grade D = trading prohibited ────────────────────────
        if grade == StockGrade.D:
            log.info("position_sizer_rejected", reason="grade_d_prohibited")
            return self._rejection("grade_d_prohibited")

        # ── Core calculation ───────────────────────────────────────────
        regime_risk_pct = self._risk_by_regime[regime]
        stop_loss_pct = (entry_price - stop_loss) / entry_price
        conf_mult = CONFIDENCE_MULTIPLIER.get(confidence, 0.50)
        grade_limit_pct = self._grade_limit.get(grade, 0.10)

        # Van Tharp formula: position = (capital * risk%) / stop_loss%
        raw_amount = (total_capital * regime_risk_pct * conf_mult) / stop_loss_pct

        # Cap by grade limit
        grade_cap_amount = total_capital * grade_limit_pct
        position_amount = int(min(raw_amount, grade_cap_amount))

        # Ensure position_amount is non-negative
        position_amount = max(position_amount, 0)

        # Derive quantity (whole shares, floor)
        quantity = position_amount // entry_price if entry_price > 0 else 0

        # Recalculate actual position amount from whole shares
        position_amount = quantity * entry_price
        position_pct = position_amount / total_capital if total_capital > 0 else 0.0

        # 1R = amount at risk
        risk_amount_1r = int(quantity * (entry_price - stop_loss))

        result = PositionSizeResult(
            position_amount=position_amount,
            position_pct=round(position_pct, 6),
            quantity=quantity,
            risk_amount_1r=risk_amount_1r,
            stop_loss_pct=round(stop_loss_pct, 6),
            r_multiple_target=self._r_target,
            regime_risk_pct=round(regime_risk_pct, 6),
            grade_limit_pct=round(grade_limit_pct, 6),
            confidence_multiplier=conf_mult,
        )

        log.info(
            "position_sizer_calculated",
            position_amount=result.position_amount,
            quantity=result.quantity,
            position_pct=result.position_pct,
            risk_amount_1r=result.risk_amount_1r,
        )

        return self._result_to_dict(result)

    # ── helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _result_to_dict(result: PositionSizeResult) -> Dict[str, Any]:
        return {
            "accepted": True,
            "position_amount": result.position_amount,
            "position_pct": result.position_pct,
            "quantity": result.quantity,
            "risk_amount_1r": result.risk_amount_1r,
            "stop_loss_pct": result.stop_loss_pct,
            "r_multiple_target": result.r_multiple_target,
            "regime_risk_pct": result.regime_risk_pct,
            "grade_limit_pct": result.grade_limit_pct,
            "confidence_multiplier": result.confidence_multiplier,
        }

    @staticmethod
    def _rejection(reason: str, **kwargs: Any) -> Dict[str, Any]:
        return {
            "accepted": False,
            "reason": reason,
            "position_amount": 0,
            "position_pct": 0.0,
            "quantity": 0,
            "risk_amount_1r": 0,
            "stop_loss_pct": 0.0,
            "r_multiple_target": 0.0,
            "regime_risk_pct": 0.0,
            "grade_limit_pct": 0.0,
            "confidence_multiplier": 0.0,
            **kwargs,
        }
