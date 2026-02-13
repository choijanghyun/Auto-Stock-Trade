"""
KATS Event Calendar

Manages market events (FOMC, options expiry, BOK rate decisions, earnings,
MSCI rebalancing, short-sell bans/lifts) that affect trading decisions.
Provides lookups for upcoming events, impact assessment, and cash-allocation
adjustments.

The calendar uses the ``event_calendars`` database table (see
``kats.database.models.EventCalendar``) as its persistence layer, accessed
through a repository object.

Usage:
    calendar = EventCalendar(repository)
    events = await calendar.get_upcoming_events(days_ahead=3)
    impact = await calendar.check_event_impact(date.today())
    if await calendar.is_special_event_day(date.today()):
        ...  # reduce position sizes
"""

from __future__ import annotations

from datetime import date, timedelta
from enum import Enum, unique
from typing import Any, Dict, List, Optional

from kats.utils.logger import get_logger

logger = get_logger(__name__)


# ============================================================================
# Event Type Enum
# ============================================================================

@unique
class EventType(str, Enum):
    """Categories of market-moving events tracked by the calendar."""

    OPTION_EXPIRY = "OPTION_EXPIRY"    # Monthly/quarterly options expiry
    FOMC = "FOMC"                      # US Federal Reserve rate decisions
    EARNINGS = "EARNINGS"              # Major corporate earnings releases
    BOK = "BOK"                        # Bank of Korea rate decisions
    MSCI = "MSCI"                      # MSCI index rebalancing
    SHORT_SELL = "SHORT_SELL"          # Short-selling ban/lift events


# ============================================================================
# Impact Defaults by Event Type
# ============================================================================

_DEFAULT_IMPACT: Dict[str, Dict[str, Any]] = {
    EventType.OPTION_EXPIRY: {
        "cash_adjust_pct": 20.0,
        "trading_action": "REDUCE",
        "description": "옵션 만기일 -- 변동성 증가 대비 포지션 축소",
    },
    EventType.FOMC: {
        "cash_adjust_pct": 30.0,
        "trading_action": "REDUCE",
        "description": "FOMC 금리 결정 -- 시장 방향성 불확실, 현금 비중 확대",
    },
    EventType.EARNINGS: {
        "cash_adjust_pct": 10.0,
        "trading_action": "NORMAL",
        "description": "실적 발표 -- 해당 종목 신규 진입 회피",
    },
    EventType.BOK: {
        "cash_adjust_pct": 20.0,
        "trading_action": "REDUCE",
        "description": "한은 금통위 -- 금리 결정에 따른 변동성 대비",
    },
    EventType.MSCI: {
        "cash_adjust_pct": 15.0,
        "trading_action": "REDUCE",
        "description": "MSCI 리밸런싱 -- 수급 변동 대비 주의",
    },
    EventType.SHORT_SELL: {
        "cash_adjust_pct": 10.0,
        "trading_action": "NORMAL",
        "description": "공매도 관련 이벤트 -- 수급 변동 모니터링",
    },
}


# ============================================================================
# EventCalendar
# ============================================================================

class EventCalendar:
    """Market event calendar for trade-decision support.

    Parameters
    ----------
    repository:
        An async repository object that provides:

        - ``get_events_in_range(start_date, end_date) -> list``
        - ``get_events_by_date(target_date) -> list``
        - ``add_event(event_data) -> event``

        Each event object/dict should have at minimum: ``event_date``,
        ``event_type``, ``event_name``, ``market_impact``,
        ``trading_action``, ``cash_adjust_pct``.
    """

    def __init__(self, repository: Any) -> None:
        self._repo = repository

    # ── Query Methods ────────────────────────────────────────────────────

    async def get_upcoming_events(
        self,
        days_ahead: int = 3,
    ) -> List[Dict[str, Any]]:
        """Retrieve market events within the next ``days_ahead`` days.

        Args:
            days_ahead: Look-ahead window in calendar days (default 3).

        Returns:
            A list of event dictionaries sorted by date ascending.
        """
        today = date.today()
        end_date = today + timedelta(days=days_ahead)

        try:
            events = await self._repo.get_events_in_range(today, end_date)
        except Exception:
            logger.exception(
                "event_calendar_query_failed",
                start=str(today),
                end=str(end_date),
            )
            return []

        result = [self._event_to_dict(e) for e in events]

        logger.info(
            "upcoming_events_fetched",
            days_ahead=days_ahead,
            count=len(result),
        )
        return result

    async def check_event_impact(
        self,
        target_date: date,
    ) -> Dict[str, Any]:
        """Assess the aggregate impact of events on a given date.

        When multiple events overlap, the *most conservative* action is
        chosen (highest ``cash_adjust_pct``, most restrictive
        ``trading_action``).

        Args:
            target_date: The date to evaluate.

        Returns:
            A dict with:
            - ``has_events`` (bool)
            - ``events`` (list of event dicts)
            - ``cash_adjust_pct`` (float) -- recommended cash increase
            - ``trading_action`` (str) -- ``"HALT"``, ``"REDUCE"``, or
              ``"NORMAL"``
            - ``description`` (str) -- combined rationale
        """
        try:
            events = await self._repo.get_events_by_date(target_date)
        except Exception:
            logger.exception(
                "event_impact_query_failed",
                date=str(target_date),
            )
            return {
                "has_events": False,
                "events": [],
                "cash_adjust_pct": 0.0,
                "trading_action": "NORMAL",
                "description": "",
            }

        if not events:
            return {
                "has_events": False,
                "events": [],
                "cash_adjust_pct": 0.0,
                "trading_action": "NORMAL",
                "description": "해당 일자에 특이 이벤트가 없습니다.",
            }

        event_dicts = [self._event_to_dict(e) for e in events]

        # Aggregate: take the maximum cash adjustment
        max_cash_pct: float = 0.0
        actions: list[str] = []
        descriptions: list[str] = []

        for ed in event_dicts:
            cash_pct = ed.get("cash_adjust_pct") or 0.0
            if cash_pct > max_cash_pct:
                max_cash_pct = cash_pct

            action = ed.get("trading_action", "NORMAL")
            actions.append(action)

            event_type = ed.get("event_type", "")
            default = _DEFAULT_IMPACT.get(event_type, {})
            desc = default.get("description", ed.get("event_name", ""))
            descriptions.append(desc)

        # Most restrictive action: HALT > REDUCE > NORMAL
        action_priority = {"HALT": 3, "REDUCE": 2, "NORMAL": 1}
        final_action = max(
            actions,
            key=lambda a: action_priority.get(a, 0),
        )

        logger.info(
            "event_impact_assessed",
            date=str(target_date),
            event_count=len(event_dicts),
            cash_adjust_pct=max_cash_pct,
            trading_action=final_action,
        )

        return {
            "has_events": True,
            "events": event_dicts,
            "cash_adjust_pct": max_cash_pct,
            "trading_action": final_action,
            "description": " | ".join(descriptions),
        }

    async def is_special_event_day(self, target_date: date) -> bool:
        """Check whether the given date has any high-impact events.

        An event day is considered "special" if there is at least one
        event with ``market_impact == "HIGH"`` or ``trading_action`` in
        ``{"HALT", "REDUCE"}``.

        Args:
            target_date: The date to check.

        Returns:
            ``True`` if the date has high-impact events.
        """
        impact = await self.check_event_impact(target_date)

        if not impact["has_events"]:
            return False

        # Check for high-impact markers
        if impact["trading_action"] in ("HALT", "REDUCE"):
            return True

        for event in impact["events"]:
            if event.get("market_impact") == "HIGH":
                return True

        return False

    # ── Mutation ─────────────────────────────────────────────────────────

    async def add_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Add a new event to the calendar.

        Required keys in ``event_data``:
        - ``event_date`` (date or str ``YYYY-MM-DD``)
        - ``event_type`` (str matching ``EventType``)
        - ``event_name`` (str)

        Optional keys:
        - ``market_impact`` (str ``HIGH``/``MEDIUM``/``LOW``)
        - ``trading_action`` (str ``HALT``/``REDUCE``/``NORMAL``)
        - ``cash_adjust_pct`` (float)

        If optional fields are omitted, defaults from ``_DEFAULT_IMPACT``
        are applied based on the event type.

        Args:
            event_data: Event attributes dictionary.

        Returns:
            The saved event as a dictionary.
        """
        event_type = event_data.get("event_type", "")

        # Apply defaults for optional fields
        defaults = _DEFAULT_IMPACT.get(event_type, {})
        if "market_impact" not in event_data:
            event_data["market_impact"] = "MEDIUM"
        if "trading_action" not in event_data:
            event_data["trading_action"] = defaults.get(
                "trading_action", "NORMAL"
            )
        if "cash_adjust_pct" not in event_data:
            event_data["cash_adjust_pct"] = defaults.get(
                "cash_adjust_pct", 0.0
            )

        # Ensure event_date is a date object
        raw_date = event_data.get("event_date")
        if isinstance(raw_date, str):
            event_data["event_date"] = date.fromisoformat(raw_date)

        try:
            saved = await self._repo.add_event(event_data)
            result = self._event_to_dict(saved)
            logger.info(
                "event_added",
                event_name=event_data.get("event_name"),
                event_date=str(event_data.get("event_date")),
                event_type=event_type,
            )
            return result
        except Exception:
            logger.exception(
                "event_add_failed",
                event_data=event_data,
            )
            raise

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _event_to_dict(event: Any) -> Dict[str, Any]:
        """Convert an event ORM model or dict to a plain dictionary.

        Handles both attribute-based objects (ORM models) and plain dicts.
        """
        if isinstance(event, dict):
            return event

        return {
            "event_id": getattr(event, "event_id", None),
            "event_date": str(getattr(event, "event_date", "")),
            "event_type": getattr(event, "event_type", ""),
            "event_name": getattr(event, "event_name", ""),
            "market_impact": getattr(event, "market_impact", None),
            "trading_action": getattr(event, "trading_action", None),
            "cash_adjust_pct": getattr(event, "cash_adjust_pct", None),
            "is_active": getattr(event, "is_active", True),
        }
