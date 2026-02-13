"""
KATS PerformanceAnalyzer - SQN, Expectancy & Strategy/Grade Breakdown

Implements Van Tharp's System Quality Number (SQN) and standard trading
performance metrics.  All methods are pure functions that accept lists of
trades or R-multiples -- no database dependency.

References:
- Van K. Tharp, "Trade Your Way to Financial Freedom" -- SQN formula
- Van K. Tharp, "Definitive Guide to Position Sizing" -- quality bands
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence

import structlog

from kats.database.models import Trade

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# SQN quality bands (Van Tharp)
# ---------------------------------------------------------------------------

_SQN_BANDS: List[tuple[float, str]] = [
    (7.0, "holy_grail"),   # >=7.0  -- likely overfitting, verify
    (5.0, "outstanding"),  # 5.0-7.0
    (3.0, "excellent"),    # 3.0-5.0
    (2.0, "good"),         # 2.0-3.0
    (1.6, "below_avg"),    # 1.6-2.0
]
_SQN_DEFAULT_BAND = "bad"  # <1.6


def _sqn_quality(sqn: float) -> str:
    """Map an SQN value to its Van Tharp quality label."""
    for threshold, label in _SQN_BANDS:
        if sqn >= threshold:
            return label
    return _SQN_DEFAULT_BAND


class PerformanceAnalyzer:
    """
    Stateless performance calculator.

    Every public method is a classmethod / staticmethod so callers do not
    need to instantiate the class; however an instance is perfectly fine
    when injected via DI for consistency with the rest of the codebase.
    """

    # ------------------------------------------------------------------
    # SQN (System Quality Number)
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_sqn(r_multiples: Sequence[float]) -> Dict[str, Any]:
        """
        Calculate Van Tharp's System Quality Number.

        SQN = (mean(R) / std(R)) * sqrt(N)

        Parameters
        ----------
        r_multiples : sequence of float
            A list of R-multiple values for closed trades.

        Returns
        -------
        dict
            sqn : float | None
                The SQN value.  ``None`` when fewer than 30 trades.
            avg_r : float
                Arithmetic mean of R-multiples.
            std_r : float
                Population-corrected standard deviation of R-multiples.
            trade_count : int
                Number of R-multiples provided.
            quality : str
                Van Tharp quality label (bad / below_avg / good /
                excellent / outstanding / holy_grail).
            win_rate : float
                Fraction of R-multiples > 0 (0.0 -- 1.0).
        """
        n = len(r_multiples)

        if n == 0:
            logger.warning("sqn_no_trades", msg="R-multiple 목록이 비어 있습니다.")
            return {
                "sqn": None,
                "avg_r": 0.0,
                "std_r": 0.0,
                "trade_count": 0,
                "quality": _SQN_DEFAULT_BAND,
                "win_rate": 0.0,
            }

        avg_r = sum(r_multiples) / n
        wins = sum(1 for r in r_multiples if r > 0)
        win_rate = wins / n

        # Sample standard deviation (Bessel correction, ddof=1)
        if n >= 2:
            variance = sum((r - avg_r) ** 2 for r in r_multiples) / (n - 1)
            std_r = math.sqrt(variance)
        else:
            std_r = 0.0

        # SQN requires minimum 30 trades for statistical significance
        if n < 30:
            logger.info(
                "sqn_insufficient_trades",
                trade_count=n,
                required=30,
                msg=f"SQN 계산에 최소 30건 필요 (현재 {n}건)",
            )
            return {
                "sqn": None,
                "avg_r": round(avg_r, 4),
                "std_r": round(std_r, 4),
                "trade_count": n,
                "quality": _SQN_DEFAULT_BAND,
                "win_rate": round(win_rate, 4),
            }

        if std_r == 0.0:
            # All trades have identical R -- degenerate case
            logger.warning(
                "sqn_zero_std",
                avg_r=avg_r,
                msg="R-multiple 표준편차 0 -- 모든 거래 동일 R",
            )
            sqn_value = 0.0
            quality = _SQN_DEFAULT_BAND
        else:
            sqn_value = (avg_r / std_r) * math.sqrt(n)
            quality = _sqn_quality(sqn_value)

        # Holy-grail warning -- probable overfitting
        if quality == "holy_grail":
            logger.warning(
                "sqn_holy_grail_warning",
                sqn=round(sqn_value, 4),
                trade_count=n,
                msg=(
                    "SQN >= 7.0 -- 과적합(overfitting) 가능성 점검 필요. "
                    "실전 vs 백테스트 차이를 확인하세요."
                ),
            )

        return {
            "sqn": round(sqn_value, 4),
            "avg_r": round(avg_r, 4),
            "std_r": round(std_r, 4),
            "trade_count": n,
            "quality": quality,
            "win_rate": round(win_rate, 4),
        }

    # ------------------------------------------------------------------
    # General metrics
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_metrics(trades: Sequence[Trade]) -> Dict[str, Any]:
        """
        Calculate standard trading performance metrics.

        Parameters
        ----------
        trades : sequence of Trade
            Closed trades with ``pnl_amount`` populated.

        Returns
        -------
        dict
            win_rate : float
            avg_win : float
            avg_loss : float
            profit_factor : float
            expectancy : float (average PnL per trade)
            max_consecutive_wins : int
            max_consecutive_losses : int
            sharpe_ratio : float | None
        """
        if not trades:
            return _empty_metrics()

        wins: List[float] = []
        losses: List[float] = []
        pnls: List[float] = []

        for t in trades:
            pnl = t.pnl_amount
            if pnl is None:
                continue
            pnls.append(pnl)
            if pnl > 0:
                wins.append(pnl)
            elif pnl < 0:
                losses.append(pnl)
            # pnl == 0 is a scratch / break-even -- not counted as win or loss

        total = len(pnls)
        if total == 0:
            return _empty_metrics()

        win_rate = len(wins) / total if total else 0.0
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0

        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = (
            gross_profit / gross_loss if gross_loss > 0 else float("inf")
        )

        expectancy = sum(pnls) / total

        # Consecutive streaks
        max_con_wins, max_con_losses = _max_consecutive(pnls)

        # Sharpe ratio (annualised, assuming ~250 trading days)
        sharpe = _annualised_sharpe(pnls)

        result = {
            "win_rate": round(win_rate, 4),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else None,
            "expectancy": round(expectancy, 2),
            "max_consecutive_wins": max_con_wins,
            "max_consecutive_losses": max_con_losses,
            "sharpe_ratio": sharpe,
        }

        logger.debug(
            "metrics_calculated",
            trade_count=total,
            win_rate=result["win_rate"],
            profit_factor=result["profit_factor"],
            expectancy=result["expectancy"],
        )
        return result

    # ------------------------------------------------------------------
    # Per-strategy breakdown
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_strategy_performance(
        trades: Sequence[Trade],
        strategy_code: str,
    ) -> Dict[str, Any]:
        """
        Return performance statistics for a single strategy.

        Parameters
        ----------
        trades : sequence of Trade
            The full trade list (will be filtered internally).
        strategy_code : str
            Strategy code to filter on (e.g. "S1", "VB").

        Returns
        -------
        dict
            strategy_code : str
            trade_count : int
            win_rate : float
            avg_r : float
            total_pnl : float
            avg_pnl : float
            profit_factor : float | None
            max_consecutive_wins : int
            max_consecutive_losses : int
        """
        filtered = [
            t for t in trades
            if t.strategy and t.strategy.strategy_code == strategy_code
        ]

        if not filtered:
            logger.debug(
                "strategy_no_trades",
                strategy_code=strategy_code,
            )
            return {
                "strategy_code": strategy_code,
                "trade_count": 0,
                "win_rate": 0.0,
                "avg_r": 0.0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "profit_factor": None,
                "max_consecutive_wins": 0,
                "max_consecutive_losses": 0,
            }

        pnls = [t.pnl_amount for t in filtered if t.pnl_amount is not None]
        r_multiples = [t.r_multiple for t in filtered if t.r_multiple is not None]

        total = len(pnls)
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]

        win_rate = len(wins) / total if total else 0.0
        avg_r = sum(r_multiples) / len(r_multiples) if r_multiples else 0.0
        total_pnl = sum(pnls)
        avg_pnl = total_pnl / total if total else 0.0

        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = (
            gross_profit / gross_loss if gross_loss > 0 else None
        )

        max_con_wins, max_con_losses = _max_consecutive(pnls)

        result = {
            "strategy_code": strategy_code,
            "trade_count": total,
            "win_rate": round(win_rate, 4),
            "avg_r": round(avg_r, 4),
            "total_pnl": round(total_pnl, 2),
            "avg_pnl": round(avg_pnl, 2),
            "profit_factor": round(profit_factor, 4) if profit_factor is not None else None,
            "max_consecutive_wins": max_con_wins,
            "max_consecutive_losses": max_con_losses,
        }

        logger.debug(
            "strategy_performance_calculated",
            **result,
        )
        return result

    # ------------------------------------------------------------------
    # Per-grade breakdown
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_grade_performance(
        trades: Sequence[Trade],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Return performance statistics grouped by stock grade (A / B / C).

        Parameters
        ----------
        trades : sequence of Trade
            The full trade list.  Grade is resolved via
            ``trade.stock.grade`` or ``trade.journal_entry.stock_grade``.

        Returns
        -------
        dict[str, dict]
            Keyed by grade ("A", "B", "C").  Each value contains:
            trade_count, win_rate, avg_r, total_pnl, avg_pnl, profit_factor.
        """
        grade_buckets: Dict[str, List[Trade]] = {"A": [], "B": [], "C": []}

        for t in trades:
            grade = _resolve_grade(t)
            if grade in grade_buckets:
                grade_buckets[grade].append(t)

        results: Dict[str, Dict[str, Any]] = {}

        for grade, bucket in grade_buckets.items():
            pnls = [t.pnl_amount for t in bucket if t.pnl_amount is not None]
            r_multiples = [t.r_multiple for t in bucket if t.r_multiple is not None]

            total = len(pnls)
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p < 0]

            win_rate = len(wins) / total if total else 0.0
            avg_r = sum(r_multiples) / len(r_multiples) if r_multiples else 0.0
            total_pnl = sum(pnls)
            avg_pnl = total_pnl / total if total else 0.0

            gross_profit = sum(wins)
            gross_loss = abs(sum(losses))
            profit_factor = (
                gross_profit / gross_loss if gross_loss > 0 else None
            )

            results[grade] = {
                "grade": grade,
                "trade_count": total,
                "win_rate": round(win_rate, 4),
                "avg_r": round(avg_r, 4),
                "total_pnl": round(total_pnl, 2),
                "avg_pnl": round(avg_pnl, 2),
                "profit_factor": round(profit_factor, 4) if profit_factor is not None else None,
            }

        logger.debug(
            "grade_performance_calculated",
            grades={g: r["trade_count"] for g, r in results.items()},
        )
        return results


# ======================================================================
# Module-level helper functions
# ======================================================================


def _empty_metrics() -> Dict[str, Any]:
    """Return a zeroed-out metrics dict."""
    return {
        "win_rate": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "profit_factor": None,
        "expectancy": 0.0,
        "max_consecutive_wins": 0,
        "max_consecutive_losses": 0,
        "sharpe_ratio": None,
    }


def _max_consecutive(pnls: List[float]) -> tuple[int, int]:
    """
    Return ``(max_consecutive_wins, max_consecutive_losses)`` from a PnL list.

    Scratch trades (pnl == 0) reset both counters.
    """
    max_wins = 0
    max_losses = 0
    current_wins = 0
    current_losses = 0

    for pnl in pnls:
        if pnl > 0:
            current_wins += 1
            current_losses = 0
            max_wins = max(max_wins, current_wins)
        elif pnl < 0:
            current_losses += 1
            current_wins = 0
            max_losses = max(max_losses, current_losses)
        else:
            # Break-even resets both streaks
            current_wins = 0
            current_losses = 0

    return max_wins, max_losses


def _annualised_sharpe(
    pnls: List[float],
    trading_days: int = 250,
) -> Optional[float]:
    """
    Calculate the annualised Sharpe ratio (risk-free rate = 0).

    Returns ``None`` if fewer than 2 data points or zero standard deviation.
    """
    n = len(pnls)
    if n < 2:
        return None

    mean_pnl = sum(pnls) / n
    variance = sum((p - mean_pnl) ** 2 for p in pnls) / (n - 1)
    std_pnl = math.sqrt(variance)

    if std_pnl == 0.0:
        return None

    daily_sharpe = mean_pnl / std_pnl
    annualised = daily_sharpe * math.sqrt(trading_days)
    return round(annualised, 4)


def _resolve_grade(trade: Trade) -> Optional[str]:
    """
    Resolve the stock grade for a trade.

    Prefers the journal-entry snapshot grade (point-in-time) over the
    current stock master grade, since grades can change over time.
    """
    # 1. Journal entry snapshot (most accurate for historical review)
    if trade.journal_entry and trade.journal_entry.stock_grade:
        return trade.journal_entry.stock_grade

    # 2. Stock master (current grade)
    if trade.stock and trade.stock.grade:
        return trade.stock.grade

    return None
