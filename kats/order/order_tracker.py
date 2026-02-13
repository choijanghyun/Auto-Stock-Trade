"""
주문 추적기 (Order Tracker).

미체결 주문을 주기적으로 감시하여 TTL 만료 시 자동 취소하고,
TTL 80% 도달 시 시장가로 정정(amend)을 시도한다.
WebSocket 체결 알림을 수신하여 상태 머신에 반영하는 역할도 담당한다.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

import structlog

from kats.api.kis_rest_client import KISRestClient
from kats.order.order_state_machine import (
    OrderState,
    OrderStateMachine,
    InvalidStateTransition,
)

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_ORDER_TTL_SEC: int = 300  # 5분

# 전략별 TTL (초) -- 전략 코드에 따라 차별적 TTL을 적용한다.
STRATEGY_TTL: Dict[str, int] = {
    "VB": 60,    # 변동성 돌파: 빠른 체결 필요, 1분
    "S2": 120,   # Gap & Go: 2분
    "GR": 600,   # 그리드 매매: 여유 있게 10분
}

# 주문 점검 주기 (초)
_CHECK_INTERVAL_SEC: float = 10.0

# TTL 대비 정정 시도 비율 (80%)
_AMEND_THRESHOLD_RATIO: float = 0.80


# ---------------------------------------------------------------------------
# OrderTracker
# ---------------------------------------------------------------------------


class OrderTracker:
    """
    미체결 주문 추적기.

    10초 주기로 미체결 주문을 순회하며:
    1. TTL 경과 시 자동 취소
    2. TTL 80% 경과 시 시장가 정정 시도
    3. 부분 체결 후 잔여 수량 자동 취소

    WebSocket 체결 알림(on_fill_notification)이 도착하면
    즉시 상태 머신에 FILLED / PARTIAL_FILLED 전이를 반영한다.
    """

    def __init__(
        self,
        state_machine: OrderStateMachine,
        rest_client: KISRestClient,
    ) -> None:
        self._state_machine = state_machine
        self._rest_client = rest_client
        self._tracking_task: Optional[asyncio.Task[None]] = None
        self._running: bool = False

        logger.info(
            "order_tracker_initialized",
            default_ttl_sec=DEFAULT_ORDER_TTL_SEC,
            strategy_ttls=STRATEGY_TTL,
            check_interval_sec=_CHECK_INTERVAL_SEC,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start_tracking(self) -> None:
        """
        비동기 추적 루프를 시작한다.

        10초 간격으로 ``_check_pending_orders()``를 호출하는
        백그라운드 asyncio Task를 생성한다.
        """
        if self._running:
            logger.warning("order_tracker_already_running")
            return

        self._running = True
        self._tracking_task = asyncio.ensure_future(self._tracking_loop())
        logger.info("order_tracker_started", interval_sec=_CHECK_INTERVAL_SEC)

    def stop_tracking(self) -> None:
        """
        추적 루프를 중단한다.

        실행 중인 Task를 cancel하고 정리한다.
        """
        self._running = False
        if self._tracking_task is not None and not self._tracking_task.done():
            self._tracking_task.cancel()
            logger.info("order_tracker_stopped")
        self._tracking_task = None

    async def _tracking_loop(self) -> None:
        """주기적으로 미체결 주문을 점검하는 메인 루프."""
        logger.info("order_tracker_loop_started")
        try:
            while self._running:
                try:
                    await self._check_pending_orders()
                except Exception:
                    logger.exception("order_tracker_check_error")
                await asyncio.sleep(_CHECK_INTERVAL_SEC)
        except asyncio.CancelledError:
            logger.info("order_tracker_loop_cancelled")

    # ------------------------------------------------------------------
    # Pending order check
    # ------------------------------------------------------------------

    async def _check_pending_orders(self) -> None:
        """
        미체결 주문을 순회하며 TTL 기반 조치를 실행한다.

        - TTL 100% 경과: 자동 취소
        - TTL 80% 경과 + 미정정: 시장가 정정 시도
        - 부분 체결 + TTL 100% 경과: 잔여 수량 취소
        """
        pending = self._state_machine.get_pending_orders()
        if not pending:
            return

        now = time.time()

        for order in pending:
            order_id: str = order["order_id"]
            created_at: float = order["created_at"]
            strategy_code: str = order.get("strategy_code", "")
            state: OrderState = order["state"]

            # 전략별 TTL 결정
            ttl = STRATEGY_TTL.get(strategy_code, DEFAULT_ORDER_TTL_SEC)
            elapsed = now - created_at
            ttl_ratio = elapsed / ttl if ttl > 0 else 1.0

            logger.debug(
                "order_tracker_check",
                order_id=order_id,
                state=state.value,
                strategy=strategy_code,
                ttl_sec=ttl,
                elapsed_sec=round(elapsed, 1),
                ttl_ratio=round(ttl_ratio, 2),
            )

            # 1) TTL 만료 -> 자동 취소
            if ttl_ratio >= 1.0:
                if state == OrderState.PARTIAL_FILLED:
                    await self._cancel_remaining(order_id, order)
                else:
                    await self._cancel_order(order_id, order)
                continue

            # 2) TTL 80% 도달 + 아직 정정 안 됨 -> 시장가 정정
            if (
                ttl_ratio >= _AMEND_THRESHOLD_RATIO
                and state == OrderState.SUBMITTED
                and not order.get("_amended", False)
            ):
                await self._amend_to_market_price(order_id, order)

    # ------------------------------------------------------------------
    # Cancel / Amend helpers
    # ------------------------------------------------------------------

    async def _cancel_order(
        self,
        order_id: str,
        order: Dict[str, Any],
    ) -> None:
        """
        미체결 주문을 REST API로 취소하고 상태 머신을 전이한다.

        Args:
            order_id: 주문 식별자.
            order: 주문 딕셔너리.
        """
        stock_code: str = order.get("stock_code", "")
        broker_order_no: str = order.get("broker_order_no", "")

        logger.info(
            "order_tracker_cancel_request",
            order_id=order_id,
            stock_code=stock_code,
            reason="TTL 만료",
        )

        try:
            # 상태 머신: SUBMITTED -> CANCEL_REQUESTED
            self._state_machine.transition(
                order_id,
                OrderState.CANCEL_REQUESTED,
                metadata={"cancel_reason": "TTL 만료 자동 취소"},
            )

            # REST API 취소 요청
            if broker_order_no:
                result = await self._rest_client.cancel_order(
                    order_no=broker_order_no,
                    stock_code=stock_code,
                )
                logger.info(
                    "order_tracker_cancel_success",
                    order_id=order_id,
                    broker_order_no=broker_order_no,
                    result_msg=result.get("msg1", ""),
                )

            # 상태 머신: CANCEL_REQUESTED -> CANCELLED
            self._state_machine.transition(
                order_id,
                OrderState.CANCELLED,
                metadata={"cancel_reason": "TTL 만료 자동 취소", "cancelled_by": "order_tracker"},
            )

        except InvalidStateTransition as exc:
            logger.warning(
                "order_tracker_cancel_transition_failed",
                order_id=order_id,
                error=str(exc),
            )
        except Exception:
            logger.exception(
                "order_tracker_cancel_failed",
                order_id=order_id,
                stock_code=stock_code,
            )

    async def _amend_to_market_price(
        self,
        order_id: str,
        order: Dict[str, Any],
    ) -> None:
        """
        주문을 시장가로 정정하여 빠른 체결을 유도한다.

        TTL 80% 경과 시점에 호출되며, 정정 후 ``_amended`` 플래그를 설정하여
        중복 정정을 방지한다.

        Args:
            order_id: 주문 식별자.
            order: 주문 딕셔너리.
        """
        stock_code: str = order.get("stock_code", "")
        broker_order_no: str = order.get("broker_order_no", "")

        logger.info(
            "order_tracker_amend_to_market",
            order_id=order_id,
            stock_code=stock_code,
            reason="TTL 80% 경과, 시장가 정정",
        )

        try:
            # 상태 머신: SUBMITTED -> AMEND_REQUESTED (부분체결은 직접 가능)
            current_state: OrderState = order["state"]
            if current_state == OrderState.SUBMITTED:
                # SUBMITTED에서는 직접 AMEND_REQUESTED로 전이 불가 -- 설계상 PARTIAL_FILLED만 가능
                # 대신 시장가 정정 REST만 호출하고, KIS 응답으로 상태 갱신
                pass

            if current_state == OrderState.PARTIAL_FILLED:
                self._state_machine.transition(
                    order_id,
                    OrderState.AMEND_REQUESTED,
                    metadata={"amend_reason": "시장가 정정 (TTL 80%)"},
                )

            # REST API 시장가 정정 (order_dvsn="01"=시장가, price=0)
            if broker_order_no:
                result = await self._rest_client.modify_order(
                    order_no=broker_order_no,
                    stock_code=stock_code,
                    new_price=0,
                    order_dvsn="01",
                )
                logger.info(
                    "order_tracker_amend_success",
                    order_id=order_id,
                    broker_order_no=broker_order_no,
                    result_msg=result.get("msg1", ""),
                )

                # 정정 후 SUBMITTED 상태로 복귀 (AMEND_REQUESTED -> SUBMITTED)
                if current_state == OrderState.PARTIAL_FILLED:
                    self._state_machine.transition(
                        order_id,
                        OrderState.SUBMITTED,
                        metadata={"amend_result": "시장가 정정 완료"},
                    )

            # 중복 정정 방지 플래그
            order["_amended"] = True

        except InvalidStateTransition as exc:
            logger.warning(
                "order_tracker_amend_transition_failed",
                order_id=order_id,
                error=str(exc),
            )
        except Exception:
            logger.exception(
                "order_tracker_amend_failed",
                order_id=order_id,
                stock_code=stock_code,
            )

    async def _cancel_remaining(
        self,
        order_id: str,
        order: Dict[str, Any],
    ) -> None:
        """
        부분 체결 주문의 잔여 수량을 취소한다.

        Args:
            order_id: 주문 식별자.
            order: 주문 딕셔너리.
        """
        stock_code: str = order.get("stock_code", "")
        broker_order_no: str = order.get("broker_order_no", "")
        filled_qty: int = order.get("filled_quantity", 0)
        total_qty: int = order.get("quantity", 0)
        remaining: int = total_qty - filled_qty

        logger.info(
            "order_tracker_cancel_remaining",
            order_id=order_id,
            stock_code=stock_code,
            filled_qty=filled_qty,
            remaining_qty=remaining,
            reason="부분 체결 후 TTL 만료, 잔여 취소",
        )

        try:
            self._state_machine.transition(
                order_id,
                OrderState.CANCEL_REQUESTED,
                metadata={
                    "cancel_reason": "부분 체결 잔여 취소",
                    "filled_quantity": filled_qty,
                    "remaining_quantity": remaining,
                },
            )

            if broker_order_no:
                await self._rest_client.cancel_order(
                    order_no=broker_order_no,
                    stock_code=stock_code,
                )

            self._state_machine.transition(
                order_id,
                OrderState.CANCELLED,
                metadata={
                    "cancel_reason": "부분 체결 잔여 취소 완료",
                    "filled_quantity": filled_qty,
                    "remaining_quantity": remaining,
                    "cancelled_by": "order_tracker",
                },
            )

        except InvalidStateTransition as exc:
            logger.warning(
                "order_tracker_cancel_remaining_transition_failed",
                order_id=order_id,
                error=str(exc),
            )
        except Exception:
            logger.exception(
                "order_tracker_cancel_remaining_failed",
                order_id=order_id,
                stock_code=stock_code,
            )

    # ------------------------------------------------------------------
    # Capital query
    # ------------------------------------------------------------------

    def get_locked_capital(self) -> float:
        """
        미체결 주문에 잠긴 예약 자본 총액을 계산한다.

        각 미체결 주문의 ``price * quantity`` 합산 금액을 반환한다.
        부분 체결된 주문은 잔여 수량만 계산한다.

        Returns:
            잠긴 자본 총액 (원).
        """
        pending = self._state_machine.get_pending_orders()
        total_locked: float = 0.0

        for order in pending:
            price: float = float(order.get("price", 0))
            quantity: int = int(order.get("quantity", 0))
            filled_qty: int = int(order.get("filled_quantity", 0))
            order_type: str = order.get("order_type", "")

            # 매수 주문만 자본을 잠금 (매도는 보유 주식을 잠금)
            if order_type == "BUY":
                remaining = quantity - filled_qty
                total_locked += price * remaining

        logger.debug(
            "locked_capital_calculated",
            total_locked=total_locked,
            pending_count=len(pending),
        )
        return total_locked

    # ------------------------------------------------------------------
    # WebSocket fill callback
    # ------------------------------------------------------------------

    def on_fill_notification(self, data: Dict[str, Any]) -> None:
        """
        WebSocket 체결 알림 콜백.

        KIS WebSocket H0STCNC0 체결 통보를 수신하여
        상태 머신에 FILLED 또는 PARTIAL_FILLED 전이를 반영한다.

        Args:
            data: WebSocket 체결 알림 데이터.
                Expected keys:
                - order_id 또는 odno: 주문번호
                - tot_ccld_qty: 총 체결 수량
                - tot_ccld_amt: 총 체결 금액
                - ccld_prc: 체결 단가
                - rmn_qty: 잔여 수량
        """
        order_id: str = data.get("order_id", "") or data.get("odno", "")
        if not order_id:
            logger.warning("fill_notification_missing_order_id", data=data)
            return

        try:
            order = self._state_machine.get_order(order_id)
        except KeyError:
            logger.warning(
                "fill_notification_unknown_order",
                order_id=order_id,
            )
            return

        total_filled: int = int(data.get("tot_ccld_qty", 0))
        remaining: int = int(data.get("rmn_qty", 0))
        fill_price: float = float(data.get("ccld_prc", 0))
        fill_amount: float = float(data.get("tot_ccld_amt", 0))

        metadata: Dict[str, Any] = {
            "fill_price": fill_price,
            "fill_amount": fill_amount,
            "filled_quantity": total_filled,
            "remaining_quantity": remaining,
            "fill_source": "websocket",
        }

        try:
            if remaining <= 0:
                # 전량 체결
                self._state_machine.transition(
                    order_id,
                    OrderState.FILLED,
                    metadata=metadata,
                )
                logger.info(
                    "order_fully_filled",
                    order_id=order_id,
                    fill_price=fill_price,
                    total_filled=total_filled,
                    stock_code=order.get("stock_code"),
                )
            else:
                # 부분 체결
                current_state: OrderState = order["state"]
                if current_state == OrderState.SUBMITTED:
                    self._state_machine.transition(
                        order_id,
                        OrderState.PARTIAL_FILLED,
                        metadata=metadata,
                    )
                elif current_state == OrderState.PARTIAL_FILLED:
                    # 이미 부분 체결 상태 -- 메타데이터만 갱신
                    order.update(metadata)
                    order["updated_at"] = time.time()

                logger.info(
                    "order_partial_filled",
                    order_id=order_id,
                    fill_price=fill_price,
                    total_filled=total_filled,
                    remaining=remaining,
                    stock_code=order.get("stock_code"),
                )

        except InvalidStateTransition as exc:
            logger.warning(
                "fill_notification_transition_failed",
                order_id=order_id,
                error=str(exc),
            )

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        """추적 루프 실행 여부."""
        return self._running

    def __repr__(self) -> str:
        pending = len(self._state_machine.get_pending_orders())
        return (
            f"OrderTracker(running={self._running}, "
            f"pending_orders={pending})"
        )
