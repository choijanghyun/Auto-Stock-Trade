"""
KATS Global Position Lock v1.1 -- Atomic Position Reservation

Thread/coroutine-safe position reservation system that prevents multiple
strategies from exceeding per-stock and per-grade exposure limits.

Hard caps (regardless of strategy combination):
    A: 30% of capital
    B: 20% of capital
    C: 10% of capital

Each reservation is tagged with the strategy code that made it, allowing
per-strategy breakdown of exposure.

References:
    - Minervini, single-stock concentration limit
    - Design doc v1.1, Section 4.3: Global Position Lock
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import structlog

from kats.config.constants import StockGrade

logger = structlog.get_logger(__name__)


# ── Hard caps per grade (% of capital) ─────────────────────────────────

GRADE_HARD_CAP: Dict[str, float] = {
    StockGrade.A: 30.0,
    StockGrade.A.value: 30.0,
    StockGrade.B: 20.0,
    StockGrade.B.value: 20.0,
    StockGrade.C: 10.0,
    StockGrade.C.value: 10.0,
    StockGrade.D: 0.0,
    StockGrade.D.value: 0.0,
}


@dataclass
class StockReservation:
    """Tracks all reservations for a single stock."""
    stock_code: str
    grade: str
    # strategy_code -> reserved_pct
    reservations: Dict[str, float] = field(default_factory=dict)

    @property
    def total_pct(self) -> float:
        return sum(self.reservations.values())


class GlobalPositionLock:
    """
    Lock-based atomic position reservation manager.

    Ensures that the aggregate exposure to any single stock across all
    strategies does not breach the grade hard cap.

    Usage::

        lock = GlobalPositionLock()
        ok, msg = await lock.check_and_reserve("005930", "A", 15.0, "S3")
        ...
        await lock.release("005930", "S3")
    """

    def __init__(
        self,
        grade_caps: Dict[str, float] | None = None,
    ) -> None:
        self._grade_caps = grade_caps or GRADE_HARD_CAP
        self._reservations: Dict[str, StockReservation] = {}
        self._lock = asyncio.Lock()

        logger.info(
            "global_position_lock_initialized",
            grade_caps={k: v for k, v in self._grade_caps.items() if isinstance(k, str)},
        )

    # ── Core API ───────────────────────────────────────────────────────

    async def check_and_reserve(
        self,
        stock_code: str,
        grade: str,
        additional_pct: float,
        strategy_code: str,
    ) -> Tuple[bool, str]:
        """
        Atomically check whether ``additional_pct`` can be reserved for
        ``stock_code`` by ``strategy_code``, and reserve it if allowed.

        Args:
            stock_code: KRX 6-digit stock code.
            grade: Stock grade (A/B/C).
            additional_pct: % of total capital to reserve.
            strategy_code: Requesting strategy identifier.

        Returns:
            (ok: bool, message: str)
        """
        async with self._lock:
            cap = self._get_cap(grade)
            reservation = self._reservations.get(stock_code)

            current_total = reservation.total_pct if reservation else 0.0
            projected = current_total + additional_pct

            log = logger.bind(
                stock_code=stock_code,
                grade=grade,
                strategy_code=strategy_code,
                additional_pct=additional_pct,
                current_total=current_total,
                projected=projected,
                cap=cap,
            )

            if projected > cap:
                remaining = max(0.0, cap - current_total)
                msg = (
                    f"Position lock denied: {stock_code} ({grade}) "
                    f"would reach {projected:.1f}% (cap {cap:.1f}%). "
                    f"Current: {current_total:.1f}%, requested: {additional_pct:.1f}%, "
                    f"remaining capacity: {remaining:.1f}%."
                )
                log.warning("global_lock_denied", reason=msg)
                return False, msg

            # Reserve
            if reservation is None:
                reservation = StockReservation(
                    stock_code=stock_code, grade=grade,
                )
                self._reservations[stock_code] = reservation

            prev = reservation.reservations.get(strategy_code, 0.0)
            reservation.reservations[strategy_code] = prev + additional_pct

            msg = (
                f"Reserved {additional_pct:.1f}% for {stock_code} "
                f"by {strategy_code}. Total: {reservation.total_pct:.1f}%."
            )
            log.info("global_lock_reserved", total=reservation.total_pct)
            return True, msg

    async def release(
        self,
        stock_code: str,
        strategy_code: str,
    ) -> Tuple[bool, str]:
        """
        Release the reservation held by ``strategy_code`` for ``stock_code``.

        Args:
            stock_code: KRX 6-digit stock code.
            strategy_code: Strategy identifier to release.

        Returns:
            (ok: bool, message: str)
        """
        async with self._lock:
            reservation = self._reservations.get(stock_code)

            if reservation is None:
                msg = f"No reservation found for {stock_code}"
                logger.warning("global_lock_release_not_found", msg=msg)
                return False, msg

            released_pct = reservation.reservations.pop(strategy_code, 0.0)

            if released_pct == 0.0:
                msg = (
                    f"No reservation for {stock_code} by {strategy_code}"
                )
                logger.warning("global_lock_release_not_found", msg=msg)
                return False, msg

            # Clean up empty reservations
            if not reservation.reservations:
                del self._reservations[stock_code]

            remaining = reservation.total_pct if reservation.reservations else 0.0
            msg = (
                f"Released {released_pct:.1f}% for {stock_code} "
                f"by {strategy_code}. Remaining: {remaining:.1f}%."
            )
            logger.info(
                "global_lock_released",
                stock_code=stock_code,
                strategy_code=strategy_code,
                released_pct=released_pct,
                remaining=remaining,
            )
            return True, msg

    # ── Query API ──────────────────────────────────────────────────────

    async def get_stock_exposure(
        self, stock_code: str,
    ) -> Dict[str, Any]:
        """
        Get current exposure breakdown for a stock.

        Returns:
            dict with ``total_pct`` and ``strategies`` (per-strategy breakdown).
        """
        async with self._lock:
            reservation = self._reservations.get(stock_code)
            if reservation is None:
                return {
                    "stock_code": stock_code,
                    "total_pct": 0.0,
                    "strategies": {},
                }
            return {
                "stock_code": stock_code,
                "total_pct": reservation.total_pct,
                "strategies": dict(reservation.reservations),
            }

    async def get_all_exposures(self) -> Dict[str, Dict[str, Any]]:
        """Return exposure breakdown for all reserved stocks."""
        async with self._lock:
            return {
                code: {
                    "total_pct": res.total_pct,
                    "grade": res.grade,
                    "strategies": dict(res.reservations),
                }
                for code, res in self._reservations.items()
            }

    async def get_remaining_capacity(
        self, stock_code: str, grade: str,
    ) -> float:
        """Return how much more % can be reserved for the given stock."""
        async with self._lock:
            cap = self._get_cap(grade)
            reservation = self._reservations.get(stock_code)
            current = reservation.total_pct if reservation else 0.0
            return max(0.0, cap - current)

    # ── Admin ──────────────────────────────────────────────────────────

    async def clear_all(self) -> None:
        """Release all reservations.  Use during daily reset."""
        async with self._lock:
            count = len(self._reservations)
            self._reservations.clear()
            logger.info("global_lock_cleared_all", count=count)

    # ── Internal ───────────────────────────────────────────────────────

    def _get_cap(self, grade: str) -> float:
        """Look up the hard cap for the given grade string or enum."""
        return self._grade_caps.get(grade, 0.0)

    def __repr__(self) -> str:
        total_stocks = len(self._reservations)
        return f"GlobalPositionLock(stocks={total_stocks})"
