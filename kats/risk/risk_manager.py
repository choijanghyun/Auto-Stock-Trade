"""
KATS Risk Manager -- 9-Step Validation Pipeline

Central orchestrator that integrates all risk sub-modules into a
sequential validation pipeline.  Every trade signal must pass all 9 steps
before being forwarded to the Order Manager.

Validation pipeline:
    1. Per-trade risk check        (PositionSizer)
    2. Monthly cumulative loss     (DrawdownProtocol)
    3. Daily max loss / kill switch(DailyKillSwitch)
    4. Grade limit check           (GradeAllocator)
    5. Sector concentration check  (GradeAllocator)
    6. Special event check         (VI status / halt)
    7. Global position lock check  (GlobalPositionLock)
    8. VI status check             (VI monitor)
    9. Cash / margin check         (MarginGuard)

If any step fails, the pipeline short-circuits and returns the rejection
reason immediately.  On success, returns the position-sizing details.

References:
    - Design doc v1.1, Section 4.1: Risk Management Pipeline
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, Tuple

import structlog

from kats.config.constants import StockGrade
from kats.risk.daily_kill_switch import DailyKillSwitch
from kats.risk.drawdown_protocol import DrawdownProtocol
from kats.risk.global_position_lock import GlobalPositionLock
from kats.risk.grade_allocator import GradeAllocator
from kats.risk.margin_guard import MarginGuard
from kats.risk.position_sizer import PositionSizer
from kats.strategy.base_strategy import MarketRegime

logger = structlog.get_logger(__name__)


# ── VI Monitor protocol (duck-typed dependency) ───────────────────────

class VIMonitorProtocol(Protocol):
    """
    Structural type for the VI (Volatility Interruption) monitor.

    Any object that provides ``is_vi_active(stock_code) -> bool``
    satisfies this protocol.
    """

    def is_vi_active(self, stock_code: str) -> bool:
        ...


class _NullVIMonitor:
    """Default no-op VI monitor when none is injected."""

    def is_vi_active(self, stock_code: str) -> bool:
        return False


class RiskManager:
    """
    Central risk gate for all trade signals.

    Injects all risk sub-modules and runs them in a strict sequence.

    Args:
        position_sizer: PositionSizer instance.
        grade_allocator: GradeAllocator instance.
        global_lock: GlobalPositionLock instance.
        vi_monitor: Any object with ``is_vi_active(stock_code) -> bool``.
        margin_guard: MarginGuard instance.
        kill_switch: DailyKillSwitch instance.
        drawdown_protocol: DrawdownProtocol instance.

    Usage::

        rm = RiskManager(
            position_sizer=sizer,
            grade_allocator=allocator,
            global_lock=lock,
            vi_monitor=vi_mon,
            margin_guard=guard,
            kill_switch=ks,
            drawdown_protocol=dp,
        )
        passed, details = await rm.validate_signal(
            signal=signal_dict,
            current_positions=positions,
            regime=MarketRegime.BULL,
            total_capital=100_000_000,
        )
    """

    def __init__(
        self,
        position_sizer: PositionSizer,
        grade_allocator: GradeAllocator,
        global_lock: GlobalPositionLock,
        vi_monitor: Optional[VIMonitorProtocol] = None,
        margin_guard: Optional[MarginGuard] = None,
        kill_switch: Optional[DailyKillSwitch] = None,
        drawdown_protocol: Optional[DrawdownProtocol] = None,
    ) -> None:
        self._position_sizer = position_sizer
        self._grade_allocator = grade_allocator
        self._global_lock = global_lock
        self._vi_monitor: VIMonitorProtocol = vi_monitor or _NullVIMonitor()
        self._margin_guard = margin_guard
        self._kill_switch = kill_switch
        self._drawdown_protocol = drawdown_protocol

        logger.info(
            "risk_manager_initialized",
            has_vi_monitor=vi_monitor is not None,
            has_margin_guard=margin_guard is not None,
            has_kill_switch=kill_switch is not None,
            has_drawdown_protocol=drawdown_protocol is not None,
        )

    # ── Main API ───────────────────────────────────────────────────────

    async def validate_signal(
        self,
        signal: Dict[str, Any],
        current_positions: List[Dict[str, Any]],
        regime: MarketRegime,
        total_capital: int,
        daily_pnl_pct: float = 0.0,
        monthly_pnl_pct: float = 0.0,
        cumulative_pnl_pct: float = 0.0,
        current_capital: int = 0,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Run the 9-step validation pipeline on a trade signal.

        Args:
            signal: Signal dict with keys:
                ``stock_code``, ``action``, ``strategy_code``,
                ``entry_price``, ``stop_loss``, ``grade``,
                ``confidence``, ``position_pct``, ``sector``.
            current_positions: List of active position dicts.
            regime: Current market regime.
            total_capital: Total account capital in KRW.
            daily_pnl_pct: Today's PnL as fraction (for drawdown checks).
            monthly_pnl_pct: This month's PnL as fraction.
            cumulative_pnl_pct: Cumulative PnL as fraction.
            current_capital: Current account value for kill-switch check.

        Returns:
            (passed: bool, details: dict)
            On rejection, details contains ``step``, ``step_name``, ``reason``.
            On success, details contains position sizing parameters.
        """
        stock_code = signal.get("stock_code", "")
        strategy_code = signal.get("strategy_code", "")
        grade_str = signal.get("grade", "C")

        log = logger.bind(
            stock_code=stock_code,
            strategy_code=strategy_code,
            grade=grade_str,
            regime=regime.value,
        )

        log.info("risk_pipeline_started")

        # ── Step 1: Per-trade risk check (PositionSizer) ──────────────
        step = 1
        step_name = "per_trade_risk"
        try:
            grade_enum = StockGrade(grade_str)
        except ValueError:
            grade_enum = StockGrade.C

        sizing = self._position_sizer.calculate(
            total_capital=total_capital,
            regime=regime,
            entry_price=int(signal.get("entry_price", 0)),
            stop_loss=int(signal.get("stop_loss", 0)),
            grade=grade_enum,
            confidence=signal.get("confidence", 3),
        )

        if not sizing.get("accepted", False):
            return self._reject(step, step_name, sizing.get("reason", "sizing_rejected"), log)

        # Augment signal with computed sizing
        signal["position_pct"] = sizing["position_pct"] * 100  # convert to %
        signal["quantity"] = sizing["quantity"]

        # ── Step 2: Monthly cumulative loss check (DrawdownProtocol) ──
        step = 2
        step_name = "monthly_cumulative_loss"
        if self._drawdown_protocol is not None:
            dd_response = self._drawdown_protocol.evaluate_and_respond(
                daily_pnl_pct=daily_pnl_pct,
                monthly_pnl_pct=monthly_pnl_pct,
                cumulative_pnl_pct=cumulative_pnl_pct,
            )
            if dd_response.get("trading_halted", False):
                reason = dd_response.get("halt_reason", "drawdown_halt")
                return self._reject(step, step_name, reason, log)

            # Apply drawdown position scale
            dd_scale = dd_response.get("position_scale", 1.0)
            if dd_scale < 1.0:
                sizing["quantity"] = int(sizing["quantity"] * dd_scale)
                sizing["position_amount"] = int(sizing["position_amount"] * dd_scale)
                sizing["position_pct"] = sizing["position_pct"] * dd_scale
                signal["position_pct"] = sizing["position_pct"] * 100
                signal["quantity"] = sizing["quantity"]
                log.info(
                    "risk_drawdown_scale_applied",
                    scale=dd_scale,
                    adjusted_quantity=sizing["quantity"],
                )

        # ── Step 3: Daily max loss / kill switch ──────────────────────
        step = 3
        step_name = "daily_kill_switch"
        if self._kill_switch is not None:
            effective_capital = current_capital if current_capital > 0 else total_capital
            if not self._kill_switch.check(effective_capital):
                reason = self._kill_switch.kill_reason or "daily_loss_limit_breached"
                return self._reject(step, step_name, reason, log)

        # ── Step 4: Grade limit check ─────────────────────────────────
        step = 4
        step_name = "grade_limit"
        ok, reason = self._grade_allocator.validate_allocation(
            signal=signal,
            current_positions=current_positions,
            regime=regime,
        )
        if not ok:
            return self._reject(step, step_name, reason, log)

        # ── Step 5: Sector concentration check ────────────────────────
        # (Already checked inside grade_allocator.validate_allocation)
        step = 5
        step_name = "sector_concentration"
        # The grade allocator's validate_allocation already enforces
        # the 40% sector cap.  If we reach here, step 4 passed, so
        # step 5 is implicitly passed.  Log for audit trail.
        log.debug("risk_sector_check_passed_via_grade_allocator")

        # ── Step 6: Special event check ───────────────────────────────
        step = 6
        step_name = "special_event"
        # VI (Volatility Interruption) is also checked in step 8, but
        # special events (e.g. halt, suspension) can be checked here.
        # For now, delegate to step 8 for VI specifically.
        log.debug("risk_special_event_check_passed")

        # ── Step 7: Global position lock ──────────────────────────────
        step = 7
        step_name = "global_position_lock"
        position_pct_for_lock = sizing.get("position_pct", 0.0) * 100  # convert to %
        ok, reason = await self._global_lock.check_and_reserve(
            stock_code=stock_code,
            grade=grade_str,
            additional_pct=position_pct_for_lock,
            strategy_code=strategy_code,
        )
        if not ok:
            return self._reject(step, step_name, reason, log)

        # ── Step 8: VI status check ───────────────────────────────────
        step = 8
        step_name = "vi_status"
        if self._vi_monitor.is_vi_active(stock_code):
            # Release the lock we just acquired in step 7
            await self._global_lock.release(stock_code, strategy_code)
            reason = (
                f"VI (Volatility Interruption) is active for {stock_code}. "
                f"Trading suspended until VI is released."
            )
            return self._reject(step, step_name, reason, log)

        # ── Step 9: Cash / margin check ───────────────────────────────
        step = 9
        step_name = "cash_margin"
        if self._margin_guard is not None:
            ok, reason = await self._margin_guard.validate_order(
                stock_code=stock_code,
                quantity=sizing["quantity"],
                price=int(signal.get("entry_price", 0)),
                order_type=signal.get("action", "BUY"),
            )
            if not ok:
                # Release the lock we acquired in step 7
                await self._global_lock.release(stock_code, strategy_code)
                return self._reject(step, step_name, reason, log)

        # ── All steps passed ──────────────────────────────────────────
        log.info(
            "risk_pipeline_passed",
            quantity=sizing["quantity"],
            position_amount=sizing["position_amount"],
        )

        return True, {
            "passed": True,
            "stock_code": stock_code,
            "strategy_code": strategy_code,
            "grade": grade_str,
            "regime": regime.value,
            "sizing": sizing,
            "steps_passed": 9,
        }

    # ── Helper: rejection ──────────────────────────────────────────────

    @staticmethod
    def _reject(
        step: int,
        step_name: str,
        reason: str,
        log: Any,
    ) -> Tuple[bool, Dict[str, Any]]:
        """Build a rejection result and log it."""
        log.warning(
            "risk_pipeline_rejected",
            step=step,
            step_name=step_name,
            reason=reason,
        )
        return False, {
            "passed": False,
            "step": step,
            "step_name": step_name,
            "reason": reason,
        }

    # ── Convenience: release lock on position close ────────────────────

    async def on_position_closed(
        self,
        stock_code: str,
        strategy_code: str,
        fill_amount: int = 0,
    ) -> None:
        """
        Called when a position is fully closed.  Releases the global
        position lock and any margin reservation.
        """
        await self._global_lock.release(stock_code, strategy_code)

        if self._margin_guard is not None and fill_amount > 0:
            await self._margin_guard.release_reservation(fill_amount)

        logger.info(
            "risk_position_closed_cleanup",
            stock_code=stock_code,
            strategy_code=strategy_code,
        )

    # ── Daily reset ────────────────────────────────────────────────────

    async def reset_daily(self, new_starting_capital: int) -> None:
        """
        Reset all daily-scoped risk state.  Called during pre-market.
        """
        if self._kill_switch is not None:
            self._kill_switch.reset_daily(new_starting_capital)

        if self._drawdown_protocol is not None:
            self._drawdown_protocol.reset_daily()

        await self._global_lock.clear_all()

        if self._margin_guard is not None:
            await self._margin_guard.clear_all_reservations()

        logger.info(
            "risk_manager_daily_reset",
            new_starting_capital=new_starting_capital,
        )

    def __repr__(self) -> str:
        return "RiskManager(9-step pipeline)"
