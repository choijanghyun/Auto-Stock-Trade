"""
KATS Daily Kill Switch -- Cameron Method

Emergency circuit breaker that halts all trading when daily losses exceed
the configured threshold.  Once activated, the kill switch:
    1. Cancels all pending orders
    2. Blocks any new order submission
    3. Dispatches an emergency notification

The switch resets automatically at the start of each trading day.

References:
    - Andrew Aziz / Cameron, "How to Day Trade for a Living"
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import Any, Callable, Coroutine, List, Optional

import structlog

from kats.config.constants import KST

logger = structlog.get_logger(__name__)


class DailyKillSwitch:
    """
    Daily loss circuit breaker.

    Args:
        daily_loss_limit_pct: Maximum allowable daily loss as a decimal
            fraction (e.g. 0.03 for 3%).  Default is 3%.
        starting_capital: Capital at the start of the trading day (KRW).
        on_cancel_all: Async callback to cancel all pending orders.
        on_notify: Async callback to send emergency notification.
    """

    def __init__(
        self,
        daily_loss_limit_pct: float = 0.03,
        starting_capital: int = 0,
        on_cancel_all: Optional[Callable[[], Coroutine[Any, Any, None]]] = None,
        on_notify: Optional[Callable[[str], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        self._daily_loss_limit_pct = daily_loss_limit_pct
        self._starting_capital = starting_capital
        self._on_cancel_all = on_cancel_all
        self._on_notify = on_notify

        self._is_killed: bool = False
        self._kill_date: Optional[date] = None
        self._kill_reason: str = ""
        self._kill_timestamp: Optional[datetime] = None

        logger.info(
            "daily_kill_switch_initialized",
            daily_loss_limit_pct=daily_loss_limit_pct,
            starting_capital=starting_capital,
        )

    # ── Properties ─────────────────────────────────────────────────────

    @property
    def is_killed(self) -> bool:
        """Whether the kill switch is currently active."""
        return self._is_killed

    @property
    def kill_reason(self) -> str:
        return self._kill_reason

    @property
    def daily_loss_limit_pct(self) -> float:
        return self._daily_loss_limit_pct

    # ── Core API ───────────────────────────────────────────────────────

    def check(self, current_capital: int) -> bool:
        """
        Evaluate whether the daily loss limit has been breached.

        Args:
            current_capital: Current account value in KRW.

        Returns:
            True if trading should continue (safe),
            False if the kill switch has been triggered.
        """
        if self._is_killed:
            return False

        if self._starting_capital <= 0:
            logger.warning(
                "kill_switch_no_starting_capital",
                starting_capital=self._starting_capital,
            )
            return True

        daily_pnl = current_capital - self._starting_capital
        daily_pnl_pct = daily_pnl / self._starting_capital

        if daily_pnl_pct <= -self._daily_loss_limit_pct:
            loss_pct = abs(daily_pnl_pct) * 100
            self._kill_reason = (
                f"Daily loss {loss_pct:.2f}% exceeded limit "
                f"{self._daily_loss_limit_pct * 100:.1f}% "
                f"(lost {abs(daily_pnl):,} KRW)"
            )
            logger.critical(
                "daily_kill_switch_triggered",
                daily_pnl=daily_pnl,
                daily_pnl_pct=round(daily_pnl_pct, 4),
                limit_pct=self._daily_loss_limit_pct,
                reason=self._kill_reason,
            )
            # Fire-and-forget the async shutdown; caller should await if needed
            asyncio.ensure_future(self._emergency_shutdown())
            return False

        return True

    async def _emergency_shutdown(self) -> None:
        """
        Execute emergency shutdown sequence:
            1. Set killed flag
            2. Cancel all pending orders
            3. Send emergency notification
        """
        now = datetime.now(tz=KST)
        self._is_killed = True
        self._kill_date = now.date()
        self._kill_timestamp = now

        logger.critical(
            "emergency_shutdown_started",
            kill_reason=self._kill_reason,
            timestamp=now.isoformat(),
        )

        # Step 1: Cancel all pending orders
        if self._on_cancel_all is not None:
            try:
                await self._on_cancel_all()
                logger.info("emergency_shutdown_orders_cancelled")
            except Exception:
                logger.exception("emergency_shutdown_cancel_failed")

        # Step 2: Send notification
        if self._on_notify is not None:
            try:
                message = (
                    f"[KATS EMERGENCY] Daily Kill Switch Activated\n"
                    f"Reason: {self._kill_reason}\n"
                    f"Time: {now.strftime('%Y-%m-%d %H:%M:%S KST')}\n"
                    f"All pending orders cancelled. New orders blocked."
                )
                await self._on_notify(message)
                logger.info("emergency_shutdown_notification_sent")
            except Exception:
                logger.exception("emergency_shutdown_notification_failed")

        logger.critical("emergency_shutdown_completed")

    # ── Day reset ──────────────────────────────────────────────────────

    def reset_daily(self, new_starting_capital: int) -> None:
        """
        Reset the kill switch for a new trading day.

        Should be called during pre-market initialization.

        Args:
            new_starting_capital: Account capital at start of new day.
        """
        self._is_killed = False
        self._kill_date = None
        self._kill_reason = ""
        self._kill_timestamp = None
        self._starting_capital = new_starting_capital

        logger.info(
            "daily_kill_switch_reset",
            new_starting_capital=new_starting_capital,
        )

    # ── Utility ────────────────────────────────────────────────────────

    def set_starting_capital(self, capital: int) -> None:
        """Update starting capital (e.g. after initial balance query)."""
        self._starting_capital = capital

    def __repr__(self) -> str:
        return (
            f"DailyKillSwitch("
            f"limit={self._daily_loss_limit_pct*100:.1f}%, "
            f"killed={self._is_killed}, "
            f"starting_capital={self._starting_capital:,})"
        )
