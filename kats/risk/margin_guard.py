"""
KATS Margin Guard v1.1 -- Cash Sufficiency & Order Parameter Enforcement

Validates that sufficient cash is available before submitting buy orders,
accounting for commissions, taxes, and pending reservations.

Key responsibilities:
    1. Validate cash availability for buy orders (sell orders pass through)
    2. Cache balance queries (5-second TTL) to avoid excessive API calls
    3. Track pending reservations to prevent over-commitment
    4. Force cash-only order parameters (no margin)

Fee structure (Korean domestic equities):
    - Commission: 0.015% (buy and sell)
    - Securities transaction tax: 0.18% (sell only, but budgeted on buy)

References:
    - KIS API balance inquiry: ``/uapi/domestic-stock/v1/trading/inquire-balance``
    - Design doc v1.1, Section 4.5: Margin Guard
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple

import structlog

logger = structlog.get_logger(__name__)

# ── Fee constants ──────────────────────────────────────────────────────

COMMISSION_RATE: float = 0.00015   # 0.015%
TAX_RATE: float = 0.0018          # 0.18% (securities transaction tax)

# Combined worst-case fee rate applied to buy-side budgeting
# (commission on buy + commission on sell + tax on sell)
_TOTAL_FEE_RATE: float = COMMISSION_RATE * 2 + TAX_RATE

# Balance cache TTL
_BALANCE_CACHE_TTL_SEC: float = 5.0


class MarginGuard:
    """
    Pre-trade cash validation guard.

    Args:
        get_balance_fn: Async callable that returns the current available
            cash balance in KRW from the broker REST API.  Signature:
            ``async () -> int``.
    """

    def __init__(
        self,
        get_balance_fn: Optional[
            Callable[[], Coroutine[Any, Any, int]]
        ] = None,
    ) -> None:
        self._get_balance_fn = get_balance_fn

        # Balance cache
        self._cached_balance: int = 0
        self._cache_timestamp: float = 0.0

        # Pending reservations (order_id -> amount_krw)
        self._pending_reservations: Dict[str, int] = {}
        self._lock = asyncio.Lock()

        logger.info(
            "margin_guard_initialized",
            commission_rate=COMMISSION_RATE,
            tax_rate=TAX_RATE,
            total_fee_rate=_TOTAL_FEE_RATE,
        )

    # ── Core API ───────────────────────────────────────────────────────

    async def validate_order(
        self,
        stock_code: str,
        quantity: int,
        price: int,
        order_type: str,
    ) -> Tuple[bool, str]:
        """
        Validate that sufficient cash is available for the order.

        Sell orders always pass without a cash check.

        Args:
            stock_code: KRX 6-digit stock code.
            quantity: Number of shares.
            price: Limit price per share in KRW.
            order_type: ``"BUY"`` or ``"SELL"``.

        Returns:
            (ok: bool, message: str)
        """
        log = logger.bind(
            stock_code=stock_code,
            quantity=quantity,
            price=price,
            order_type=order_type,
        )

        # Sell orders pass unconditionally
        if order_type.upper() == "SELL":
            log.debug("margin_guard_sell_pass")
            return True, "Sell order: no cash check required."

        # Calculate required amount including fees
        gross_amount = quantity * price
        fee_amount = int(gross_amount * _TOTAL_FEE_RATE)
        required_amount = gross_amount + fee_amount

        log = log.bind(
            gross_amount=gross_amount,
            fee_amount=fee_amount,
            required_amount=required_amount,
        )

        # Get available cash (cached)
        available = await self._get_available_cash()

        if available < required_amount:
            shortfall = required_amount - available
            msg = (
                f"Insufficient cash for {stock_code}: "
                f"required {required_amount:,} KRW "
                f"(order {gross_amount:,} + fees {fee_amount:,}), "
                f"available {available:,} KRW, "
                f"shortfall {shortfall:,} KRW."
            )
            log.warning("margin_guard_insufficient", shortfall=shortfall)
            return False, msg

        # Reserve the amount
        reservation_key = f"{stock_code}_{int(time.monotonic() * 1000)}"
        async with self._lock:
            self._pending_reservations[reservation_key] = required_amount

        msg = (
            f"Cash validated for {stock_code}: "
            f"{required_amount:,} KRW reserved (available: {available:,} KRW). "
            f"Reservation: {reservation_key}."
        )
        log.info("margin_guard_validated", reservation_key=reservation_key)
        return True, msg

    async def release_reservation(self, amount: int) -> None:
        """
        Release a pending reservation after order fill or cancellation.

        Removes the oldest reservation matching or closest to ``amount``.
        If no exact match is found, removes the oldest reservation.

        Args:
            amount: KRW amount to release.
        """
        async with self._lock:
            if not self._pending_reservations:
                logger.debug("margin_guard_release_nothing_pending")
                return

            # Try to find an exact match
            for key, reserved in self._pending_reservations.items():
                if reserved == amount:
                    del self._pending_reservations[key]
                    logger.info(
                        "margin_guard_released_exact",
                        key=key,
                        amount=amount,
                    )
                    return

            # No exact match -- remove the oldest (first inserted)
            oldest_key = next(iter(self._pending_reservations))
            released = self._pending_reservations.pop(oldest_key)
            logger.info(
                "margin_guard_released_oldest",
                key=oldest_key,
                released=released,
                requested=amount,
            )

    async def release_reservation_by_key(self, reservation_key: str) -> None:
        """Release a specific reservation by its key."""
        async with self._lock:
            released = self._pending_reservations.pop(reservation_key, None)
            if released is not None:
                logger.info(
                    "margin_guard_released_by_key",
                    key=reservation_key,
                    amount=released,
                )
            else:
                logger.debug(
                    "margin_guard_release_key_not_found",
                    key=reservation_key,
                )

    # ── Order parameter enforcement ────────────────────────────────────

    @staticmethod
    def enforce_cash_order_params(order_body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Force cash-only order parameters on the given KIS order body.

        Overrides any margin-related fields to ensure the order is placed
        as a regular cash order (no credit / margin trading).

        Args:
            order_body: Mutable KIS REST API order request body.

        Returns:
            The modified order_body dict.
        """
        # KIS API cash-only parameters
        order_body["ORD_DVSN"] = "00"           # 00 = limit order (cash)
        order_body["CTAC_TLNO"] = ""             # No contact phone (automated)
        order_body["SLL_TYPE"] = "01"            # 01 = cash sell
        order_body["ALGO_NO"] = ""               # No algorithm
        order_body.pop("CANO_LOAN", None)        # Remove any loan account
        order_body.pop("MGNT_DVSN", None)        # Remove margin division
        order_body.pop("LOAN_DT", None)          # Remove loan date

        logger.debug("margin_guard_enforced_cash_params")
        return order_body

    # ── Balance query with cache ───────────────────────────────────────

    async def _get_available_cash(self) -> int:
        """
        Get available cash, using cached value if fresh enough.

        Available = broker_balance - sum(pending_reservations)
        """
        now = time.monotonic()

        # Refresh cache if stale
        if now - self._cache_timestamp > _BALANCE_CACHE_TTL_SEC:
            if self._get_balance_fn is not None:
                try:
                    self._cached_balance = await self._get_balance_fn()
                    self._cache_timestamp = now
                    logger.debug(
                        "margin_guard_balance_refreshed",
                        balance=self._cached_balance,
                    )
                except Exception:
                    logger.exception("margin_guard_balance_query_failed")
                    # Use stale cache on failure
            else:
                logger.warning("margin_guard_no_balance_fn")

        # Subtract pending reservations
        async with self._lock:
            total_reserved = sum(self._pending_reservations.values())

        available = max(0, self._cached_balance - total_reserved)
        return available

    # ── Query helpers ──────────────────────────────────────────────────

    async def get_pending_total(self) -> int:
        """Return total KRW currently reserved in pending orders."""
        async with self._lock:
            return sum(self._pending_reservations.values())

    async def get_pending_count(self) -> int:
        """Return number of pending reservations."""
        async with self._lock:
            return len(self._pending_reservations)

    def set_balance(self, balance: int) -> None:
        """Manually set the cached balance (for testing or initialization)."""
        self._cached_balance = balance
        self._cache_timestamp = time.monotonic()

    async def clear_all_reservations(self) -> None:
        """Clear all pending reservations. Use during daily reset."""
        async with self._lock:
            count = len(self._pending_reservations)
            self._pending_reservations.clear()
            logger.info("margin_guard_cleared_all", count=count)

    def __repr__(self) -> str:
        return (
            f"MarginGuard(balance={self._cached_balance:,}, "
            f"pending={len(self._pending_reservations)})"
        )
