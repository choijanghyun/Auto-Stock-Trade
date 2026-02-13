"""
KATS Strategy Base Module

Provides the abstract base class, enums, and dataclasses used by every
concrete trading strategy in the system.

Design references:
- MarketRegime detection: Elder (Triple Screen 1st filter) + O'Neil (M factor)
- Position sizing: Van Tharp (R-multiple)
- Grade-based allocation: Minervini (stock selection quality)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


# ───────────────────────────── Enums ─────────────────────────────────────────


class MarketRegime(Enum):
    """Current market regime classification.

    Determined by KOSPI MA50/MA200 relationship and advance-decline ratio.
    Korean labels stored as values for logging and notification readability.
    """

    STRONG_BULL = "강한 상승장"
    BULL = "일반 상승장"
    SIDEWAYS = "보합/횡보장"
    BEAR = "약세장"
    STRONG_BEAR = "강한 하락장"


class StrategyCategory(Enum):
    """Broad category that determines in which regimes a strategy operates."""

    BULL = "상승장"      # S1~S5
    BEAR = "하락장"      # B1~B4
    NEUTRAL = "중립"     # Grid, Dividend, Volatility Breakout


# ───────────────────────────── Data Classes ──────────────────────────────────


@dataclass
class TradeSignal:
    """Immutable record of a trade signal emitted by a strategy.

    Attributes:
        stock_code: 6-digit KRX stock code (e.g. ``"005930"``).
        action: ``"BUY"`` or ``"SELL"``.
        strategy_code: Short code identifying the source strategy
            (``S1`` .. ``S5``, ``B1`` .. ``B4``, ``VB``, ``GR``, ``DS``).
        entry_price: Recommended entry price in KRW.
        stop_loss: Hard stop-loss price in KRW.
        target_prices: Progressive take-profit levels.  A value of ``0``
            means trailing stop instead of a fixed target.
        position_pct: Suggested position size as a percentage of capital.
        confidence: Conviction score from 1 (low) to 5 (high).
        reason: Human-readable rationale for the signal (Korean).
        indicators_snapshot: Dictionary snapshot of all relevant indicator
            values at signal creation time (for trade journal).
    """

    stock_code: str
    action: str
    strategy_code: str
    entry_price: float
    stop_loss: float
    target_prices: List[float]
    position_pct: float
    confidence: int
    reason: str
    indicators_snapshot: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StockCandidate:
    """Pre-screened stock that passed the initial stock screener filters.

    Populated by ``market.stock_screener.StockScreener.scan_daily()`` and
    fed into each strategy's ``scan()`` method for further filtering.

    Attributes:
        stock_code: 6-digit KRX stock code.
        stock_name: Korean company name.
        market: ``"KOSPI"`` or ``"KOSDAQ"``.
        sector: KRX sector classification.
        market_cap: Market capitalisation in KRW.
        grade: Quality grade (``"A"`` / ``"B"`` / ``"C"`` / ``"ETF"``).
        price: Latest closing price.
        ma_50: 50-day simple moving average.
        ma_150: 150-day simple moving average.
        ma_200: 200-day simple moving average.
        ma_200_slope: 200-day MA slope (positive = uptrend).
        week52_high: 52-week high price.
        week52_low: 52-week low price.
        rs_rank: Relative strength percentile rank (0-100).
        avg_volume_20d: 20-day average daily volume.
        avg_turnover_20d: 20-day average daily turnover in KRW.
        eps_growth_qoq: Quarter-over-quarter EPS growth (%).
        revenue_growth: Year-over-year revenue growth (%).
        op_margin_trend: Operating margin trend (``"UP"``/``"DOWN"``/``"FLAT"``).
        inst_foreign_flow: Net institutional + foreign buying flow (KRW).
        trend_score: Composite technical trend score (0-100).
        canslim_score: CAN SLIM composite score (0-100).
        confidence: Overall confidence score (1-5).
    """

    stock_code: str
    stock_name: str
    market: str
    sector: str
    market_cap: float
    grade: str
    price: float
    ma_50: float
    ma_150: float
    ma_200: float
    ma_200_slope: float
    week52_high: float
    week52_low: float
    rs_rank: float
    avg_volume_20d: float
    avg_turnover_20d: float
    eps_growth_qoq: float
    revenue_growth: float
    op_margin_trend: str
    inst_foreign_flow: float
    trend_score: float
    canslim_score: float
    confidence: int


# ───────────────────────────── Abstract Base ─────────────────────────────────


class BaseStrategy(ABC):
    """Abstract base class that every trading strategy must extend.

    Lifecycle:
        1. ``scan()``            -- filter candidates that match strategy criteria
        2. ``generate_signal()`` -- produce a concrete BUY/SELL signal
        3. ``get_exit_rules()``  -- provide stop-loss / take-profit / trailing rules

    Subclass contract:
        * Implement all three abstract methods.
        * Call ``super().__init__(strategy_code, category)`` in ``__init__``.
        * Store strategy-specific parameters in ``self.params: dict``.
    """

    def __init__(self, strategy_code: str, category: StrategyCategory) -> None:
        self.strategy_code = strategy_code
        self.category = category
        self.is_active: bool = True
        self.log = structlog.get_logger(
            __name__,
            strategy=strategy_code,
            category=category.value,
        )

    # ── Abstract interface ────────────────────────────────────────────────

    @abstractmethod
    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Filter *candidates* to those that satisfy strategy-specific
        technical and fundamental criteria.

        Returns:
            A (possibly empty) subset of *candidates*.
        """

    @abstractmethod
    async def generate_signal(
        self,
        stock: StockCandidate,
        market_data: Dict[str, Any],
    ) -> Optional[TradeSignal]:
        """Evaluate *stock* against live *market_data* and return a
        :class:`TradeSignal` if conditions are met, else ``None``.

        ``market_data`` is a dictionary provided by the Market Data Hub
        containing at minimum::

            {
                "current_price": float,
                "current_volume": int,
                "today_open": float,
                "prev_day": {"open": ..., "high": ..., "low": ..., "close": ...},
                "daily_prices": pd.DataFrame,   # OHLCV history
                "minute_candles": pd.DataFrame,  # intraday 1-min candles
                "indicators": dict,              # pre-computed indicators
            }
        """

    @abstractmethod
    def get_exit_rules(self) -> Dict[str, Any]:
        """Return a dictionary describing exit rules so that the Risk
        Manager and Order Manager can enforce them autonomously.

        Expected keys::

            {
                "stop_loss_pct": float,
                "target_prices_pct": List[float],
                "trailing_stop": bool,
                "trailing_stop_pct": float | None,
                "time_exit": str | None,        # e.g. "MARKET_CLOSE"
                "max_holding_hours": int | None,
            }
        """

    # ── Regime applicability ──────────────────────────────────────────────

    def is_applicable(self, regime: MarketRegime) -> bool:
        """Return ``True`` if this strategy should be active under *regime*.

        * BULL strategies run in STRONG_BULL and BULL.
        * BEAR strategies run in BEAR and STRONG_BEAR.
        * NEUTRAL strategies are always applicable.
        """
        if self.category == StrategyCategory.BULL:
            return regime in (MarketRegime.STRONG_BULL, MarketRegime.BULL)
        if self.category == StrategyCategory.BEAR:
            return regime in (MarketRegime.BEAR, MarketRegime.STRONG_BEAR)
        return True  # NEUTRAL

    # ── Position sizing helpers ───────────────────────────────────────────

    def _adjust_position(self, confidence: int, grade: str) -> float:
        """Scale the base ``position_pct`` by *confidence* and stock *grade*.

        Multipliers:
            * **Grade**: A = 1.0, B = 0.8, C = 0.5, ETF = 0.7
            * **Confidence**: ``confidence / 5``  (i.e. 5 -> 100 %, 1 -> 20 %)

        Returns:
            Adjusted position percentage (float, e.g. ``17.5``).
        """
        base_pct: float = getattr(self, "params", {}).get("position_pct", 10.0)

        grade_multiplier = {"A": 1.0, "B": 0.8, "C": 0.5, "ETF": 0.7}.get(
            grade, 0.5
        )
        confidence_multiplier = max(confidence, 1) / 5.0

        adjusted = base_pct * grade_multiplier * confidence_multiplier
        self.log.debug(
            "position_adjusted",
            base_pct=base_pct,
            grade=grade,
            confidence=confidence,
            adjusted=round(adjusted, 2),
        )
        return round(adjusted, 2)

    # ── Indicator snapshot helper ─────────────────────────────────────────

    @staticmethod
    def _capture_snapshot(indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Create a JSON-serialisable shallow copy of *indicators*.

        Non-serialisable values (e.g. NumPy arrays) are converted to plain
        Python types so that the snapshot can be persisted in the trade
        journal without additional transformation.
        """
        snapshot: Dict[str, Any] = {}
        for key, value in indicators.items():
            try:
                # Convert numpy scalars / arrays to builtin types
                if hasattr(value, "item"):
                    snapshot[key] = value.item()
                elif hasattr(value, "tolist"):
                    snapshot[key] = value.tolist()
                else:
                    snapshot[key] = value
            except Exception:  # noqa: BLE001
                snapshot[key] = str(value)
        return snapshot

    # ── Repr ──────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} "
            f"code={self.strategy_code} "
            f"category={self.category.value} "
            f"active={self.is_active}>"
        )
