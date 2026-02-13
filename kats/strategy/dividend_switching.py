"""
KATS Dividend Switching Strategy (DS)

Calendar-driven strategy that rotates into high-dividend stocks before
their ex-dividend dates and exploits post-ex-date oversold bounces.

Core logic:
    1. Buy 2 trading days before the ex-dividend date for stocks with
       dividend yield >= 3 %.
    2. Hold through the ex-date to capture the dividend.
    3. After the ex-date, monitor for an oversold-bounce opportunity
       (gap-down recovery) to add or rotate.

Category: NEUTRAL -- dividend capture is regime-independent.

References:
    - Geraldine Weiss, "Dividends Don't Lie"
    - Market calendar integration for Korean exchange dividend schedules
"""

from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

import structlog

from kats.strategy.base_strategy import (
    BaseStrategy,
    StockCandidate,
    StrategyCategory,
    TradeSignal,
)

logger = structlog.get_logger(__name__)


class DividendSwitchingStrategy(BaseStrategy):
    """Dividend-capture and post-ex-date rotation strategy.

    Maintains an internal dividend calendar that maps stock codes to
    their upcoming ex-dividend dates.  The calendar should be populated
    externally via :meth:`update_dividend_calendar`.

    Parameters:
        min_dividend_yield: Minimum annual dividend yield (%, default 3.0).
        buy_days_before_ex: Trading days before ex-date to initiate buy
            (default 2).
        post_ex_rsi_threshold: RSI level below which a post-ex-date bounce
            entry is considered (default 35).
        grade_target: Eligible stock grades (default ``["A"]``).
        position_pct: Base position size (default 20 %).

    Category: NEUTRAL.
    """

    def __init__(self) -> None:
        super().__init__("DS", StrategyCategory.NEUTRAL)
        self.params: Dict[str, Any] = {
            "min_dividend_yield": 3.0,
            "buy_days_before_ex": 2,
            "post_ex_rsi_threshold": 35,
            "grade_target": ["A"],
            "position_pct": 20.0,
        }
        # {stock_code: [{"ex_date": date, "dividend_yield": float, "stock_name": str}]}
        self.dividend_calendar: Dict[str, List[Dict[str, Any]]] = {}

    # ── Calendar management ───────────────────────────────────────────────

    def update_dividend_calendar(
        self, calendar: Dict[str, List[Dict[str, Any]]]
    ) -> None:
        """Replace the internal dividend calendar.

        Expected format::

            {
                "005930": [
                    {"ex_date": date(2026, 3, 28), "dividend_yield": 3.5,
                     "stock_name": "삼성전자"},
                ],
                ...
            }
        """
        self.dividend_calendar = calendar
        self.log.info(
            "calendar_updated",
            stocks=len(calendar),
            total_events=sum(len(v) for v in calendar.values()),
        )

    # ── Internal helpers ──────────────────────────────────────────────────

    def _get_upcoming_dividends(
        self, today: datetime.date, days_ahead: int
    ) -> List[Dict[str, Any]]:
        """Return dividend events whose ex-date is within *days_ahead*
        trading days from *today*.
        """
        target_date = today + datetime.timedelta(days=days_ahead)
        upcoming: List[Dict[str, Any]] = []
        for code, events in self.dividend_calendar.items():
            for ev in events:
                ex_date = ev["ex_date"]
                if today <= ex_date <= target_date:
                    upcoming.append({**ev, "stock_code": code})
        return upcoming

    def _get_post_ex_dividend(
        self, today: datetime.date
    ) -> List[Dict[str, Any]]:
        """Return stocks whose ex-date was yesterday (post-ex-date check)."""
        yesterday = today - datetime.timedelta(days=1)
        post_ex: List[Dict[str, Any]] = []
        for code, events in self.dividend_calendar.items():
            for ev in events:
                if ev["ex_date"] == yesterday:
                    post_ex.append({**ev, "stock_code": code})
        return post_ex

    @staticmethod
    def _is_oversold_bounce(
        indicators: Dict[str, Any], rsi_threshold: float
    ) -> bool:
        """Check if post-ex-date price action shows an oversold bounce."""
        rsi = indicators.get("rsi_14", 50)
        return rsi < rsi_threshold

    # ── Scan ──────────────────────────────────────────────────────────────

    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Filter candidates to those present in the dividend calendar
        with adequate yield.
        """
        filtered: List[StockCandidate] = []
        for c in candidates:
            if c.grade not in self.params["grade_target"]:
                continue
            if c.stock_code not in self.dividend_calendar:
                continue
            # Check if any upcoming event has sufficient yield
            for ev in self.dividend_calendar[c.stock_code]:
                if ev.get("dividend_yield", 0) >= self.params["min_dividend_yield"]:
                    filtered.append(c)
                    break
        self.log.info("scan_complete", strategy="DS", matched=len(filtered))
        return filtered

    # ── Signal generation ─────────────────────────────────────────────────

    async def generate_signal(
        self,
        stock: StockCandidate,
        market_data: Dict[str, Any],
    ) -> Optional[TradeSignal]:
        indicators: Dict[str, Any] = market_data.get("indicators", {})
        current_price: float = market_data["current_price"]
        today = datetime.date.today()

        # --- Pre-ex-date buy signal ---
        upcoming = self._get_upcoming_dividends(
            today, self.params["buy_days_before_ex"]
        )
        for ev in upcoming:
            if ev["stock_code"] != stock.stock_code:
                continue
            if ev.get("dividend_yield", 0) < self.params["min_dividend_yield"]:
                continue

            stop_loss = current_price * 0.95  # 5 % protective stop

            self.log.info(
                "dividend_buy_signal",
                stock=stock.stock_code,
                ex_date=str(ev["ex_date"]),
                dividend_yield=ev["dividend_yield"],
            )
            return TradeSignal(
                stock_code=stock.stock_code,
                action="BUY",
                strategy_code="DS",
                entry_price=current_price,
                stop_loss=stop_loss,
                target_prices=[current_price * 1.03, current_price * 1.05],
                position_pct=self._adjust_position(
                    stock.confidence, stock.grade
                ),
                confidence=min(stock.confidence, 4),
                reason=(
                    f"배당 매수: 배당락일 {ev['ex_date']} "
                    f"{self.params['buy_days_before_ex']}일 전, "
                    f"배당수익률 {ev['dividend_yield']:.1f}%"
                ),
                indicators_snapshot=self._capture_snapshot(indicators),
            )

        # --- Post-ex-date bounce signal ---
        post_ex = self._get_post_ex_dividend(today)
        for ev in post_ex:
            if ev["stock_code"] != stock.stock_code:
                continue
            if not self._is_oversold_bounce(
                indicators, self.params["post_ex_rsi_threshold"]
            ):
                continue

            stop_loss = current_price * 0.97
            self.log.info(
                "post_ex_bounce_signal",
                stock=stock.stock_code,
                rsi=indicators.get("rsi_14"),
            )
            return TradeSignal(
                stock_code=stock.stock_code,
                action="BUY",
                strategy_code="DS",
                entry_price=current_price,
                stop_loss=stop_loss,
                target_prices=[
                    current_price * 1.02,
                    current_price * 1.04,
                ],
                position_pct=self._adjust_position(
                    stock.confidence, stock.grade
                ),
                confidence=min(stock.confidence, 3),
                reason=(
                    f"배당락 후 반등: RSI {indicators.get('rsi_14', 0):.1f} "
                    f"과매도 반등 기회"
                ),
                indicators_snapshot=self._capture_snapshot(indicators),
            )

        return None

    # ── Calendar-based check (alternative entry point) ────────────────────

    async def check_switching_signal(
        self, date: datetime.date, all_market_data: Dict[str, Dict[str, Any]]
    ) -> List[TradeSignal]:
        """Batch check for dividend switching opportunities on *date*.

        This is an alternative entry point called by the scheduler rather
        than the standard scan -> generate_signal pipeline.

        Args:
            date: The current trading date.
            all_market_data: Mapping of ``stock_code`` to its market_data dict.

        Returns:
            List of trade signals (may be empty).
        """
        signals: List[TradeSignal] = []

        # Pre-ex-date buys
        upcoming = self._get_upcoming_dividends(
            date, self.params["buy_days_before_ex"]
        )
        for ev in upcoming:
            code = ev["stock_code"]
            if ev.get("dividend_yield", 0) < self.params["min_dividend_yield"]:
                continue
            md = all_market_data.get(code)
            if md is None:
                continue
            price = md["current_price"]
            signals.append(
                TradeSignal(
                    stock_code=code,
                    action="BUY",
                    strategy_code="DS",
                    entry_price=price,
                    stop_loss=price * 0.95,
                    target_prices=[price * 1.03, price * 1.05],
                    position_pct=self.params["position_pct"] * 0.8,
                    confidence=3,
                    reason=(
                        f"배당 스위칭 매수: {ev.get('stock_name', code)} "
                        f"배당락일 {ev['ex_date']}, "
                        f"수익률 {ev['dividend_yield']:.1f}%"
                    ),
                    indicators_snapshot=self._capture_snapshot(
                        md.get("indicators", {})
                    ),
                )
            )

        # Post-ex-date bounces
        post_ex = self._get_post_ex_dividend(date)
        for ev in post_ex:
            code = ev["stock_code"]
            md = all_market_data.get(code)
            if md is None:
                continue
            indicators = md.get("indicators", {})
            if not self._is_oversold_bounce(
                indicators, self.params["post_ex_rsi_threshold"]
            ):
                continue
            price = md["current_price"]
            signals.append(
                TradeSignal(
                    stock_code=code,
                    action="BUY",
                    strategy_code="DS",
                    entry_price=price,
                    stop_loss=price * 0.97,
                    target_prices=[price * 1.02, price * 1.04],
                    position_pct=self.params["position_pct"] * 0.6,
                    confidence=2,
                    reason=(
                        f"배당락 후 반등: {ev.get('stock_name', code)} "
                        f"RSI {indicators.get('rsi_14', 0):.1f}"
                    ),
                    indicators_snapshot=self._capture_snapshot(indicators),
                )
            )

        self.log.info("switching_check", date=str(date), signals=len(signals))
        return signals

    # ── Exit rules ────────────────────────────────────────────────────────

    def get_exit_rules(self) -> Dict[str, Any]:
        return {
            "stop_loss_pct": 5.0,
            "target_prices_pct": [3.0, 5.0],
            "trailing_stop": False,
            "trailing_stop_pct": None,
            "time_exit": None,
            "max_holding_hours": None,
        }
