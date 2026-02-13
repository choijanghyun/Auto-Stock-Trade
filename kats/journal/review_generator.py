"""
KATS ReviewGenerator - Daily / Weekly / Monthly Performance Reviews

Generates structured review reports and formats them as Korean-language
notification text suitable for Slack, Telegram, or in-app display.

Depends on PerformanceAnalyzer for metric calculations and a repository
for data retrieval.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

import structlog

from kats.database.models import Trade, TradeJournalEntry
from kats.journal.performance_analyzer import PerformanceAnalyzer

logger = structlog.get_logger(__name__)


class ReviewGenerator:
    """
    Produces daily, weekly, and monthly trading reviews.

    Parameters
    ----------
    performance_analyzer : PerformanceAnalyzer
        Calculator used for SQN, metrics, and breakdowns.
    repository
        Async repository (or session-factory wrapper) providing:
        - ``get_trades_between(start: datetime, end: datetime) -> List[Trade]``
        - ``get_journal_entries_between(start: datetime, end: datetime) -> List[TradeJournalEntry]``
    """

    def __init__(
        self,
        performance_analyzer: PerformanceAnalyzer,
        repository: Any,
    ) -> None:
        self._analyzer = performance_analyzer
        self._repo = repository

    # ------------------------------------------------------------------
    # Daily review
    # ------------------------------------------------------------------

    async def generate_daily_review(self, review_date: date) -> Dict[str, Any]:
        """
        Generate a daily trading review.

        Parameters
        ----------
        review_date : date
            The calendar date to review.

        Returns
        -------
        dict
            review_type : "daily"
            date : str (ISO format)
            trade_count : int
            total_pnl : float
            r_multiples : list[float]
            avg_r : float
            best_trade : dict | None
            worst_trade : dict | None
            win_rate : float
            rule_violations : list[str]
        """
        start_dt = datetime.combine(review_date, datetime.min.time())
        end_dt = datetime.combine(review_date, datetime.max.time())

        trades = await self._repo.get_trades_between(start_dt, end_dt)
        entries = await self._repo.get_journal_entries_between(start_dt, end_dt)

        closed_trades = [t for t in trades if t.pnl_amount is not None]
        pnls = [t.pnl_amount for t in closed_trades]
        r_multiples = [
            t.r_multiple for t in closed_trades if t.r_multiple is not None
        ]

        total_pnl = sum(pnls) if pnls else 0.0
        avg_r = sum(r_multiples) / len(r_multiples) if r_multiples else 0.0

        # Best / worst trade
        best_trade = _trade_summary(max(closed_trades, key=lambda t: t.pnl_amount)) if closed_trades else None
        worst_trade = _trade_summary(min(closed_trades, key=lambda t: t.pnl_amount)) if closed_trades else None

        # Win rate
        wins = sum(1 for p in pnls if p > 0)
        win_rate = wins / len(pnls) if pnls else 0.0

        # Rule violations
        rule_violations = [
            e.rule_violation
            for e in entries
            if e.rule_violation
        ]

        review: Dict[str, Any] = {
            "review_type": "daily",
            "date": review_date.isoformat(),
            "trade_count": len(closed_trades),
            "total_pnl": round(total_pnl, 2),
            "r_multiples": [round(r, 4) for r in r_multiples],
            "avg_r": round(avg_r, 4),
            "best_trade": best_trade,
            "worst_trade": worst_trade,
            "win_rate": round(win_rate, 4),
            "rule_violations": rule_violations,
        }

        logger.info(
            "daily_review_generated",
            date=review_date.isoformat(),
            trade_count=review["trade_count"],
            total_pnl=review["total_pnl"],
        )
        return review

    # ------------------------------------------------------------------
    # Weekly review
    # ------------------------------------------------------------------

    async def generate_weekly_review(
        self,
        week_start: date,
    ) -> Dict[str, Any]:
        """
        Generate a weekly trading review (Monday through Friday).

        Parameters
        ----------
        week_start : date
            The Monday of the week to review.  If a non-Monday is passed,
            it is snapped back to the preceding Monday.

        Returns
        -------
        dict
            review_type : "weekly"
            period_start : str
            period_end : str
            trade_count : int
            win_count : int
            loss_count : int
            win_rate : float
            total_pnl : float
            avg_r : float
            sqn : dict  (from PerformanceAnalyzer.calculate_sqn)
            rule_violations : list[str]
            strategy_breakdown : dict[str, dict]
        """
        # Snap to Monday
        monday = week_start - timedelta(days=week_start.weekday())
        friday = monday + timedelta(days=4)

        start_dt = datetime.combine(monday, datetime.min.time())
        end_dt = datetime.combine(friday, datetime.max.time())

        trades = await self._repo.get_trades_between(start_dt, end_dt)
        entries = await self._repo.get_journal_entries_between(start_dt, end_dt)

        closed_trades = [t for t in trades if t.pnl_amount is not None]
        pnls = [t.pnl_amount for t in closed_trades]
        r_multiples = [
            t.r_multiple for t in closed_trades if t.r_multiple is not None
        ]

        total_pnl = sum(pnls) if pnls else 0.0
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        win_rate = len(wins) / len(pnls) if pnls else 0.0
        avg_r = sum(r_multiples) / len(r_multiples) if r_multiples else 0.0

        # SQN (may be None if < 30 trades -- that is expected for weekly)
        sqn_result = self._analyzer.calculate_sqn(r_multiples)

        # Rule violations
        rule_violations = [
            e.rule_violation
            for e in entries
            if e.rule_violation
        ]

        # Per-strategy breakdown
        strategy_codes = _unique_strategy_codes(closed_trades)
        strategy_breakdown: Dict[str, Dict[str, Any]] = {}
        for code in strategy_codes:
            strategy_breakdown[code] = (
                self._analyzer.calculate_strategy_performance(closed_trades, code)
            )

        review: Dict[str, Any] = {
            "review_type": "weekly",
            "period_start": monday.isoformat(),
            "period_end": friday.isoformat(),
            "trade_count": len(closed_trades),
            "win_count": len(wins),
            "loss_count": len(losses),
            "win_rate": round(win_rate, 4),
            "total_pnl": round(total_pnl, 2),
            "avg_r": round(avg_r, 4),
            "sqn": sqn_result,
            "rule_violations": rule_violations,
            "strategy_breakdown": strategy_breakdown,
        }

        logger.info(
            "weekly_review_generated",
            period=f"{monday.isoformat()} ~ {friday.isoformat()}",
            trade_count=review["trade_count"],
            total_pnl=review["total_pnl"],
            sqn=sqn_result.get("sqn"),
        )
        return review

    # ------------------------------------------------------------------
    # Monthly review
    # ------------------------------------------------------------------

    async def generate_monthly_review(
        self,
        year: int,
        month: int,
    ) -> Dict[str, Any]:
        """
        Generate a monthly trading review.

        Parameters
        ----------
        year : int
        month : int (1-12)

        Returns
        -------
        dict
            review_type : "monthly"
            year : int
            month : int
            trade_count : int
            total_pnl : float
            monthly_pnl_pct : float | None
            sqn : dict
            max_drawdown : float
            compliance_rate : float
            strategy_pnl : dict[str, float]
            grade_pnl : dict[str, float]
            metrics : dict (from calculate_metrics)
        """
        _, last_day = calendar.monthrange(year, month)
        start_dt = datetime(year, month, 1, 0, 0, 0)
        end_dt = datetime(year, month, last_day, 23, 59, 59)

        trades = await self._repo.get_trades_between(start_dt, end_dt)
        entries = await self._repo.get_journal_entries_between(start_dt, end_dt)

        closed_trades = [t for t in trades if t.pnl_amount is not None]
        pnls = [t.pnl_amount for t in closed_trades]
        r_multiples = [
            t.r_multiple for t in closed_trades if t.r_multiple is not None
        ]

        total_pnl = sum(pnls) if pnls else 0.0

        # Monthly PnL percentage (relative to first trade's capital estimate)
        monthly_pnl_pct = _estimate_monthly_pnl_pct(closed_trades, total_pnl)

        # SQN
        sqn_result = self._analyzer.calculate_sqn(r_multiples)

        # Max drawdown
        max_dd = _calculate_max_drawdown(pnls)

        # Rule compliance rate
        total_entries = len(entries)
        violations = sum(1 for e in entries if e.rule_violation)
        compliance_rate = (
            (total_entries - violations) / total_entries
            if total_entries > 0
            else 1.0
        )

        # Strategy-level PnL contribution
        strategy_codes = _unique_strategy_codes(closed_trades)
        strategy_pnl: Dict[str, float] = {}
        for code in strategy_codes:
            code_pnl = sum(
                t.pnl_amount
                for t in closed_trades
                if t.strategy
                and t.strategy.strategy_code == code
                and t.pnl_amount is not None
            )
            strategy_pnl[code] = round(code_pnl, 2)

        # Grade-level PnL contribution
        grade_performance = self._analyzer.calculate_grade_performance(closed_trades)
        grade_pnl: Dict[str, float] = {
            grade: data["total_pnl"]
            for grade, data in grade_performance.items()
        }

        # Full metrics
        metrics = self._analyzer.calculate_metrics(closed_trades)

        review: Dict[str, Any] = {
            "review_type": "monthly",
            "year": year,
            "month": month,
            "trade_count": len(closed_trades),
            "total_pnl": round(total_pnl, 2),
            "monthly_pnl_pct": round(monthly_pnl_pct, 4) if monthly_pnl_pct is not None else None,
            "sqn": sqn_result,
            "max_drawdown": round(max_dd, 4),
            "compliance_rate": round(compliance_rate, 4),
            "strategy_pnl": strategy_pnl,
            "grade_pnl": grade_pnl,
            "metrics": metrics,
        }

        logger.info(
            "monthly_review_generated",
            year=year,
            month=month,
            trade_count=review["trade_count"],
            total_pnl=review["total_pnl"],
            sqn=sqn_result.get("sqn"),
            max_drawdown=review["max_drawdown"],
            compliance_rate=review["compliance_rate"],
        )
        return review

    # ------------------------------------------------------------------
    # Text formatting (Korean)
    # ------------------------------------------------------------------

    @staticmethod
    def format_review_text(review_data: Dict[str, Any]) -> str:
        """
        Format a review dict into a Korean notification string.

        Supports daily, weekly, and monthly review types.  The output is
        plain text with light formatting suitable for Slack ``mrkdwn``
        or Telegram ``MarkdownV2`` (after escaping).

        Parameters
        ----------
        review_data : dict
            A review dict produced by ``generate_daily_review``,
            ``generate_weekly_review``, or ``generate_monthly_review``.

        Returns
        -------
        str
            Formatted Korean text.
        """
        review_type = review_data.get("review_type", "unknown")

        if review_type == "daily":
            return _format_daily(review_data)
        elif review_type == "weekly":
            return _format_weekly(review_data)
        elif review_type == "monthly":
            return _format_monthly(review_data)
        else:
            logger.warning(
                "unknown_review_type",
                review_type=review_type,
            )
            return f"[KATS] 알 수 없는 리뷰 유형: {review_type}"


# ======================================================================
# Module-level helper functions
# ======================================================================


def _trade_summary(trade: Trade) -> Dict[str, Any]:
    """Build a lightweight summary dict for a single trade."""
    return {
        "trade_id": trade.trade_id,
        "stock_code": trade.stock_code,
        "pnl_amount": trade.pnl_amount,
        "pnl_percent": trade.pnl_percent,
        "r_multiple": trade.r_multiple,
        "entry_price": trade.entry_price,
        "exit_price": trade.exit_price,
        "quantity": trade.quantity,
        "strategy": (
            trade.strategy.strategy_code if trade.strategy else None
        ),
    }


def _unique_strategy_codes(trades: Sequence[Trade]) -> List[str]:
    """Extract unique strategy codes from a list of trades, sorted."""
    codes: set[str] = set()
    for t in trades:
        if t.strategy and t.strategy.strategy_code:
            codes.add(t.strategy.strategy_code)
    return sorted(codes)


def _calculate_max_drawdown(pnls: List[float]) -> float:
    """
    Calculate maximum peak-to-trough drawdown from a sequential PnL list.

    Returns the drawdown as a positive ratio (e.g. 0.05 = 5% drawdown).
    If cumulative equity never goes negative, returns 0.0.
    """
    if not pnls:
        return 0.0

    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0

    for pnl in pnls:
        cumulative += pnl
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if peak > 0:
            dd_ratio = drawdown / peak
            max_dd = max(max_dd, dd_ratio)

    return max_dd


def _estimate_monthly_pnl_pct(
    trades: Sequence[Trade],
    total_pnl: float,
) -> Optional[float]:
    """
    Estimate monthly PnL percentage.

    Uses the first trade's ``amount`` as a rough capital proxy.
    Returns ``None`` when capital cannot be estimated.
    """
    if not trades or total_pnl == 0.0:
        return 0.0

    # Try to estimate from the largest position as a proxy
    amounts = [
        t.amount for t in trades
        if t.amount is not None and t.amount > 0
    ]
    if not amounts:
        return None

    # Use the max amount as a rough capital estimate
    estimated_capital = max(amounts) * 5  # assume max ~20% position sizing
    if estimated_capital <= 0:
        return None

    return (total_pnl / estimated_capital) * 100.0


# ======================================================================
# Formatters (Korean)
# ======================================================================

def _pnl_sign(value: float) -> str:
    """Return a value string prefixed with + or - for display."""
    if value >= 0:
        return f"+{value:,.0f}"
    return f"{value:,.0f}"


def _format_daily(data: Dict[str, Any]) -> str:
    """Format a daily review into Korean text."""
    lines = [
        f"[KATS 일일 리뷰] {data['date']}",
        "=" * 36,
        f"  총 거래: {data['trade_count']}건",
        f"  승률: {data['win_rate'] * 100:.1f}%",
        f"  총 손익: {_pnl_sign(data['total_pnl'])}원",
        f"  평균 R: {data['avg_r']:.2f}R",
    ]

    if data.get("r_multiples"):
        r_str = ", ".join(f"{r:.2f}R" for r in data["r_multiples"])
        lines.append(f"  R-multiples: [{r_str}]")

    if data.get("best_trade"):
        bt = data["best_trade"]
        lines.append(
            f"  최고 거래: {bt['stock_code']} "
            f"{_pnl_sign(bt['pnl_amount'])}원 "
            f"({bt.get('r_multiple', 'N/A')}R)"
        )

    if data.get("worst_trade"):
        wt = data["worst_trade"]
        lines.append(
            f"  최악 거래: {wt['stock_code']} "
            f"{_pnl_sign(wt['pnl_amount'])}원 "
            f"({wt.get('r_multiple', 'N/A')}R)"
        )

    violations = data.get("rule_violations", [])
    if violations:
        lines.append(f"  규칙 위반: {len(violations)}건")
        for v in violations:
            lines.append(f"    - {v}")
    else:
        lines.append("  규칙 위반: 없음")

    return "\n".join(lines)


def _format_weekly(data: Dict[str, Any]) -> str:
    """Format a weekly review into Korean text."""
    sqn_data = data.get("sqn", {})
    sqn_value = sqn_data.get("sqn")
    sqn_quality = sqn_data.get("quality", "N/A")
    sqn_display = f"{sqn_value:.2f} ({sqn_quality})" if sqn_value is not None else "N/A (30건 미만)"

    lines = [
        f"[KATS 주간 리뷰] {data['period_start']} ~ {data['period_end']}",
        "=" * 44,
        f"  총 거래: {data['trade_count']}건 (승 {data['win_count']} / 패 {data['loss_count']})",
        f"  승률: {data['win_rate'] * 100:.1f}%",
        f"  총 손익: {_pnl_sign(data['total_pnl'])}원",
        f"  평균 R: {data['avg_r']:.2f}R",
        f"  SQN: {sqn_display}",
    ]

    # Rule violations
    violations = data.get("rule_violations", [])
    lines.append(f"  규칙 위반: {len(violations)}건")

    # Strategy breakdown
    strategy_breakdown = data.get("strategy_breakdown", {})
    if strategy_breakdown:
        lines.append("")
        lines.append("  [전략별 성과]")
        for code, stats in sorted(strategy_breakdown.items()):
            pnl = stats.get("total_pnl", 0)
            wr = stats.get("win_rate", 0) * 100
            cnt = stats.get("trade_count", 0)
            lines.append(
                f"    {code}: {cnt}건, 승률 {wr:.0f}%, "
                f"손익 {_pnl_sign(pnl)}원"
            )

    return "\n".join(lines)


def _format_monthly(data: Dict[str, Any]) -> str:
    """Format a monthly review into Korean text."""
    sqn_data = data.get("sqn", {})
    sqn_value = sqn_data.get("sqn")
    sqn_quality = sqn_data.get("quality", "N/A")
    sqn_display = f"{sqn_value:.2f} ({sqn_quality})" if sqn_value is not None else "N/A (30건 미만)"

    metrics = data.get("metrics", {})
    pnl_pct = data.get("monthly_pnl_pct")
    pnl_pct_str = f"{pnl_pct:.2f}%" if pnl_pct is not None else "N/A"

    lines = [
        f"[KATS 월간 리뷰] {data['year']}년 {data['month']}월",
        "=" * 40,
        f"  총 거래: {data['trade_count']}건",
        f"  총 손익: {_pnl_sign(data['total_pnl'])}원 ({pnl_pct_str})",
        f"  SQN: {sqn_display}",
        f"  최대 낙폭(MDD): {data['max_drawdown'] * 100:.2f}%",
        f"  규칙 준수율: {data['compliance_rate'] * 100:.1f}%",
    ]

    # Core metrics
    if metrics:
        lines.append("")
        lines.append("  [핵심 지표]")
        lines.append(f"    승률: {metrics.get('win_rate', 0) * 100:.1f}%")
        lines.append(f"    평균 수익: {_pnl_sign(metrics.get('avg_win', 0))}원")
        lines.append(f"    평균 손실: {metrics.get('avg_loss', 0):,.0f}원")
        pf = metrics.get("profit_factor")
        pf_str = f"{pf:.2f}" if pf is not None else "N/A"
        lines.append(f"    손익비(Profit Factor): {pf_str}")
        lines.append(f"    기대값: {_pnl_sign(metrics.get('expectancy', 0))}원/거래")
        sharpe = metrics.get("sharpe_ratio")
        sharpe_str = f"{sharpe:.2f}" if sharpe is not None else "N/A"
        lines.append(f"    샤프비율(연환산): {sharpe_str}")
        lines.append(
            f"    최대 연승: {metrics.get('max_consecutive_wins', 0)}건 / "
            f"최대 연패: {metrics.get('max_consecutive_losses', 0)}건"
        )

    # Strategy PnL
    strategy_pnl = data.get("strategy_pnl", {})
    if strategy_pnl:
        lines.append("")
        lines.append("  [전략별 손익 기여]")
        for code in sorted(strategy_pnl.keys()):
            lines.append(f"    {code}: {_pnl_sign(strategy_pnl[code])}원")

    # Grade PnL
    grade_pnl = data.get("grade_pnl", {})
    if grade_pnl:
        lines.append("")
        lines.append("  [등급별 손익 기여]")
        for grade in ("A", "B", "C"):
            pnl_val = grade_pnl.get(grade, 0.0)
            lines.append(f"    {grade}등급: {_pnl_sign(pnl_val)}원")

    return "\n".join(lines)
