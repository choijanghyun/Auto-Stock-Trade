"""
KATS Drawdown Protocol -- 5-Level Adaptive Response

Progressively restricts trading activity as drawdown deepens:

    Level  Threshold                Action
    -----  -----------------------  ----------------------------------------
    GREEN  -2% daily                Reduce new positions by 50%
    YELLOW -3~5% daily              Halt trading for the rest of the day
    ORANGE -6% monthly (Elder 6%)   Halt trading for the rest of the month
    RED    -10% cumulative          Halt 1 week, switch to paper trading,
                                    recovery = 5 consecutive paper wins
    BLACK  -15%+ cumulative         Indefinite halt, full strategy review

References:
    - Alexander Elder, "Trading for a Living" (6% monthly loss rule)
    - Van Tharp, "Trade Your Way to Financial Freedom" (equity curve management)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, unique
from typing import Any, Dict, Optional

import structlog

from kats.config.constants import KST

logger = structlog.get_logger(__name__)


@unique
class DrawdownLevel(str, Enum):
    """Drawdown severity levels in ascending order."""

    NONE = "NONE"
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    ORANGE = "ORANGE"
    RED = "RED"
    BLACK = "BLACK"


# ── Level thresholds (absolute values, compared as positive) ───────────

_DAILY_GREEN_THRESHOLD = 0.02     # -2% daily
_DAILY_YELLOW_LOW = 0.03          # -3% daily
_DAILY_YELLOW_HIGH = 0.05         # -5% daily
_MONTHLY_ORANGE_THRESHOLD = 0.06  # -6% monthly (Elder 6%)
_CUMULATIVE_RED_THRESHOLD = 0.10  # -10% cumulative
_CUMULATIVE_BLACK_THRESHOLD = 0.15  # -15% cumulative

# Recovery requirements for RED level
RED_RECOVERY_PAPER_WINS = 5
RED_HALT_DAYS = 7


@dataclass
class DrawdownState:
    """Mutable state tracking for drawdown protocol."""

    level: DrawdownLevel = DrawdownLevel.NONE
    position_scale: float = 1.0         # 1.0 = full, 0.5 = half, 0.0 = halted
    trading_halted: bool = False
    halt_reason: str = ""
    halt_until: Optional[datetime] = None
    paper_mode_forced: bool = False
    consecutive_paper_wins: int = 0
    strategy_review_required: bool = False
    last_evaluated: Optional[datetime] = None


class DrawdownProtocol:
    """
    5-level drawdown response system.

    Evaluate current PnL at three time horizons and return the appropriate
    risk-reduction action.

    Usage::

        protocol = DrawdownProtocol()
        response = protocol.evaluate_and_respond(
            daily_pnl_pct=-0.025,
            monthly_pnl_pct=-0.04,
            cumulative_pnl_pct=-0.08,
        )
        if response["trading_halted"]:
            ...
    """

    def __init__(self) -> None:
        self._state = DrawdownState()
        logger.info("drawdown_protocol_initialized")

    # ── Properties ─────────────────────────────────────────────────────

    @property
    def current_level(self) -> DrawdownLevel:
        return self._state.level

    @property
    def is_halted(self) -> bool:
        return self._state.trading_halted

    @property
    def position_scale(self) -> float:
        return self._state.position_scale

    @property
    def state(self) -> DrawdownState:
        return self._state

    # ── Core API ───────────────────────────────────────────────────────

    def evaluate_and_respond(
        self,
        daily_pnl_pct: float,
        monthly_pnl_pct: float,
        cumulative_pnl_pct: float,
    ) -> Dict[str, Any]:
        """
        Evaluate drawdown across all three horizons and determine the
        appropriate response.

        All pnl values should be negative fractions for losses
        (e.g. -0.03 for a 3% loss).

        Args:
            daily_pnl_pct: Today's PnL as a fraction of starting capital.
            monthly_pnl_pct: Current month's PnL as a fraction.
            cumulative_pnl_pct: Cumulative PnL since inception / reset.

        Returns:
            dict with keys:
                level, position_scale, trading_halted, halt_reason,
                halt_until, paper_mode_forced, strategy_review_required,
                consecutive_paper_wins, daily_pnl_pct, monthly_pnl_pct,
                cumulative_pnl_pct
        """
        now = datetime.now(tz=KST)
        self._state.last_evaluated = now

        # Check if we're still within a timed halt period
        if self._state.halt_until and now < self._state.halt_until:
            logger.info(
                "drawdown_still_halted",
                level=self._state.level.value,
                halt_until=self._state.halt_until.isoformat(),
            )
            return self._build_response(
                daily_pnl_pct, monthly_pnl_pct, cumulative_pnl_pct
            )

        # Evaluate from most severe to least severe
        # (worst level wins, no downgrade once escalated within a session)
        new_level = self._classify(
            daily_pnl_pct, monthly_pnl_pct, cumulative_pnl_pct
        )

        # Only escalate, never de-escalate automatically
        if self._severity(new_level) > self._severity(self._state.level):
            self._escalate(new_level, daily_pnl_pct, monthly_pnl_pct,
                           cumulative_pnl_pct, now)

        return self._build_response(
            daily_pnl_pct, monthly_pnl_pct, cumulative_pnl_pct
        )

    # ── Classification ─────────────────────────────────────────────────

    @staticmethod
    def _classify(
        daily_pnl_pct: float,
        monthly_pnl_pct: float,
        cumulative_pnl_pct: float,
    ) -> DrawdownLevel:
        """Determine the drawdown level from the worst PnL horizon."""
        # BLACK: cumulative >= -15%
        if cumulative_pnl_pct <= -_CUMULATIVE_BLACK_THRESHOLD:
            return DrawdownLevel.BLACK

        # RED: cumulative >= -10%
        if cumulative_pnl_pct <= -_CUMULATIVE_RED_THRESHOLD:
            return DrawdownLevel.RED

        # ORANGE: monthly >= -6% (Elder rule)
        if monthly_pnl_pct <= -_MONTHLY_ORANGE_THRESHOLD:
            return DrawdownLevel.ORANGE

        # YELLOW: daily between -3% and -5%
        if daily_pnl_pct <= -_DAILY_YELLOW_LOW:
            return DrawdownLevel.YELLOW

        # GREEN: daily >= -2%
        if daily_pnl_pct <= -_DAILY_GREEN_THRESHOLD:
            return DrawdownLevel.GREEN

        return DrawdownLevel.NONE

    def _escalate(
        self,
        level: DrawdownLevel,
        daily_pnl_pct: float,
        monthly_pnl_pct: float,
        cumulative_pnl_pct: float,
        now: datetime,
    ) -> None:
        """Apply the actions for the given drawdown level."""
        self._state.level = level

        if level == DrawdownLevel.GREEN:
            self._state.position_scale = 0.5
            self._state.trading_halted = False
            self._state.halt_reason = (
                f"GREEN: daily loss {abs(daily_pnl_pct)*100:.1f}% >= 2%. "
                f"New positions reduced to 50%."
            )
            logger.warning(
                "drawdown_green",
                daily_pnl_pct=round(daily_pnl_pct, 4),
                action="position_scale_50pct",
            )

        elif level == DrawdownLevel.YELLOW:
            self._state.position_scale = 0.0
            self._state.trading_halted = True
            self._state.halt_reason = (
                f"YELLOW: daily loss {abs(daily_pnl_pct)*100:.1f}% >= 3%. "
                f"Trading halted for the rest of the day."
            )
            # Halt until end of day (next 16:30 KST)
            eod = now.replace(hour=16, minute=30, second=0, microsecond=0)
            if now >= eod:
                eod += timedelta(days=1)
            self._state.halt_until = eod
            logger.error(
                "drawdown_yellow",
                daily_pnl_pct=round(daily_pnl_pct, 4),
                action="halt_rest_of_day",
                halt_until=eod.isoformat(),
            )

        elif level == DrawdownLevel.ORANGE:
            self._state.position_scale = 0.0
            self._state.trading_halted = True
            self._state.halt_reason = (
                f"ORANGE (Elder 6% Rule): monthly loss "
                f"{abs(monthly_pnl_pct)*100:.1f}% >= 6%. "
                f"Trading halted for the rest of the month."
            )
            # Halt until first day of next month
            if now.month == 12:
                next_month = now.replace(
                    year=now.year + 1, month=1, day=1,
                    hour=9, minute=0, second=0, microsecond=0,
                )
            else:
                next_month = now.replace(
                    month=now.month + 1, day=1,
                    hour=9, minute=0, second=0, microsecond=0,
                )
            self._state.halt_until = next_month
            logger.error(
                "drawdown_orange",
                monthly_pnl_pct=round(monthly_pnl_pct, 4),
                action="halt_rest_of_month",
                halt_until=next_month.isoformat(),
            )

        elif level == DrawdownLevel.RED:
            self._state.position_scale = 0.0
            self._state.trading_halted = True
            self._state.paper_mode_forced = True
            self._state.consecutive_paper_wins = 0
            halt_end = now + timedelta(days=RED_HALT_DAYS)
            self._state.halt_until = halt_end
            self._state.halt_reason = (
                f"RED: cumulative loss {abs(cumulative_pnl_pct)*100:.1f}% >= 10%. "
                f"Halted {RED_HALT_DAYS} days + paper mode. "
                f"Recovery requires {RED_RECOVERY_PAPER_WINS} consecutive paper wins."
            )
            logger.critical(
                "drawdown_red",
                cumulative_pnl_pct=round(cumulative_pnl_pct, 4),
                action="halt_1_week_paper_mode",
                halt_until=halt_end.isoformat(),
            )

        elif level == DrawdownLevel.BLACK:
            self._state.position_scale = 0.0
            self._state.trading_halted = True
            self._state.paper_mode_forced = True
            self._state.strategy_review_required = True
            self._state.halt_until = None  # Indefinite
            self._state.halt_reason = (
                f"BLACK: cumulative loss {abs(cumulative_pnl_pct)*100:.1f}% >= 15%. "
                f"INDEFINITE HALT. Full strategy review required before resuming."
            )
            logger.critical(
                "drawdown_black",
                cumulative_pnl_pct=round(cumulative_pnl_pct, 4),
                action="indefinite_halt_strategy_review",
            )

    # ── Recovery (RED level) ───────────────────────────────────────────

    def record_paper_trade_result(self, win: bool) -> bool:
        """
        Record a paper trade result during RED-level recovery.

        Args:
            win: Whether the paper trade was a win.

        Returns:
            True if recovery is complete (can resume live trading).
        """
        if self._state.level != DrawdownLevel.RED:
            return False

        if win:
            self._state.consecutive_paper_wins += 1
            logger.info(
                "drawdown_paper_win",
                consecutive_wins=self._state.consecutive_paper_wins,
                required=RED_RECOVERY_PAPER_WINS,
            )
        else:
            self._state.consecutive_paper_wins = 0
            logger.info("drawdown_paper_loss", consecutive_wins=0)

        if self._state.consecutive_paper_wins >= RED_RECOVERY_PAPER_WINS:
            logger.info(
                "drawdown_red_recovery_complete",
                consecutive_wins=self._state.consecutive_paper_wins,
            )
            self._reset_to_none()
            return True

        return False

    # ── Manual overrides ───────────────────────────────────────────────

    def force_resume(self, reason: str = "manual_override") -> None:
        """
        Manually resume trading (e.g. after BLACK-level strategy review).

        Should only be called after deliberate human decision.
        """
        logger.warning(
            "drawdown_force_resume",
            previous_level=self._state.level.value,
            reason=reason,
        )
        self._reset_to_none()

    def reset_daily(self) -> None:
        """
        Reset daily-level drawdown states at start of a new trading day.

        GREEN and YELLOW are daily-scoped; higher levels persist.
        """
        if self._state.level in (DrawdownLevel.GREEN, DrawdownLevel.YELLOW):
            logger.info(
                "drawdown_daily_reset",
                previous_level=self._state.level.value,
            )
            self._reset_to_none()

    def reset_monthly(self) -> None:
        """
        Reset monthly-level drawdown states at start of a new month.

        ORANGE is monthly-scoped; RED and BLACK persist.
        """
        if self._state.level == DrawdownLevel.ORANGE:
            logger.info("drawdown_monthly_reset")
            self._reset_to_none()

    # ── Internals ──────────────────────────────────────────────────────

    def _reset_to_none(self) -> None:
        self._state.level = DrawdownLevel.NONE
        self._state.position_scale = 1.0
        self._state.trading_halted = False
        self._state.halt_reason = ""
        self._state.halt_until = None
        self._state.paper_mode_forced = False
        self._state.consecutive_paper_wins = 0
        self._state.strategy_review_required = False

    @staticmethod
    def _severity(level: DrawdownLevel) -> int:
        """Numeric severity for comparison."""
        return {
            DrawdownLevel.NONE: 0,
            DrawdownLevel.GREEN: 1,
            DrawdownLevel.YELLOW: 2,
            DrawdownLevel.ORANGE: 3,
            DrawdownLevel.RED: 4,
            DrawdownLevel.BLACK: 5,
        }[level]

    def _build_response(
        self,
        daily_pnl_pct: float,
        monthly_pnl_pct: float,
        cumulative_pnl_pct: float,
    ) -> Dict[str, Any]:
        return {
            "level": self._state.level.value,
            "position_scale": self._state.position_scale,
            "trading_halted": self._state.trading_halted,
            "halt_reason": self._state.halt_reason,
            "halt_until": (
                self._state.halt_until.isoformat()
                if self._state.halt_until
                else None
            ),
            "paper_mode_forced": self._state.paper_mode_forced,
            "strategy_review_required": self._state.strategy_review_required,
            "consecutive_paper_wins": self._state.consecutive_paper_wins,
            "daily_pnl_pct": round(daily_pnl_pct, 6),
            "monthly_pnl_pct": round(monthly_pnl_pct, 6),
            "cumulative_pnl_pct": round(cumulative_pnl_pct, 6),
        }

    def __repr__(self) -> str:
        return (
            f"DrawdownProtocol(level={self._state.level.value}, "
            f"halted={self._state.trading_halted}, "
            f"scale={self._state.position_scale})"
        )
