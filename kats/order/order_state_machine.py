"""
주문 상태 머신 (Order State Machine).

주문의 생명주기를 유한 상태 머신(FSM)으로 관리한다.
유효하지 않은 상태 전이를 차단하고, 상태 변경 시 등록된 콜백을 실행한다.
모든 상태 변경 이력은 주문 내부 history 리스트에 기록된다.
"""

from __future__ import annotations

import time
import uuid
from enum import Enum, unique
from typing import Any, Callable, Dict, List, Optional, Set

import structlog

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# OrderState Enum
# ---------------------------------------------------------------------------


@unique
class OrderState(str, Enum):
    """주문 상태 열거형 -- 각 상태에 한글 라벨을 포함한다."""

    CREATED = "CREATED"                  # 생성됨
    SUBMITTED = "SUBMITTED"              # 접수됨
    PARTIAL_FILLED = "PARTIAL_FILLED"    # 부분 체결
    FILLED = "FILLED"                    # 전량 체결
    CANCEL_REQUESTED = "CANCEL_REQUESTED"  # 취소 요청
    CANCELLED = "CANCELLED"              # 취소 완료
    AMEND_REQUESTED = "AMEND_REQUESTED"  # 정정 요청
    REJECTED = "REJECTED"                # 거부됨
    EXPIRED = "EXPIRED"                  # 만료됨
    ERROR = "ERROR"                      # 오류

    @property
    def label_kr(self) -> str:
        """한글 라벨을 반환한다."""
        return _STATE_LABELS_KR[self]


_STATE_LABELS_KR: Dict[OrderState, str] = {
    OrderState.CREATED: "생성됨",
    OrderState.SUBMITTED: "접수됨",
    OrderState.PARTIAL_FILLED: "부분 체결",
    OrderState.FILLED: "전량 체결",
    OrderState.CANCEL_REQUESTED: "취소 요청",
    OrderState.CANCELLED: "취소 완료",
    OrderState.AMEND_REQUESTED: "정정 요청",
    OrderState.REJECTED: "거부됨",
    OrderState.EXPIRED: "만료됨",
    OrderState.ERROR: "오류",
}


# ---------------------------------------------------------------------------
# Valid state transitions
# ---------------------------------------------------------------------------

VALID_TRANSITIONS: Dict[OrderState, Set[OrderState]] = {
    OrderState.CREATED: {OrderState.SUBMITTED, OrderState.REJECTED},
    OrderState.SUBMITTED: {
        OrderState.PARTIAL_FILLED,
        OrderState.FILLED,
        OrderState.CANCEL_REQUESTED,
        OrderState.REJECTED,
        OrderState.ERROR,
    },
    OrderState.PARTIAL_FILLED: {
        OrderState.FILLED,
        OrderState.CANCEL_REQUESTED,
        OrderState.AMEND_REQUESTED,
    },
    OrderState.CANCEL_REQUESTED: {OrderState.CANCELLED, OrderState.FILLED},
    OrderState.AMEND_REQUESTED: {OrderState.SUBMITTED, OrderState.REJECTED},
}

# Terminal states -- no outgoing transitions
_TERMINAL_STATES: frozenset[OrderState] = frozenset({
    OrderState.FILLED,
    OrderState.CANCELLED,
    OrderState.REJECTED,
    OrderState.EXPIRED,
    OrderState.ERROR,
})

# States that fire on_order_completed callback
_COMPLETED_STATES: frozenset[OrderState] = frozenset({
    OrderState.FILLED,
    OrderState.CANCELLED,
    OrderState.EXPIRED,
})


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class InvalidStateTransition(Exception):
    """유효하지 않은 상태 전이를 시도했을 때 발생하는 예외.

    Attributes:
        order_id: 주문 식별자.
        current_state: 현재 상태.
        target_state: 시도한 대상 상태.
    """

    def __init__(
        self,
        order_id: str,
        current_state: OrderState,
        target_state: OrderState,
    ) -> None:
        self.order_id = order_id
        self.current_state = current_state
        self.target_state = target_state
        super().__init__(
            f"주문 {order_id}: {current_state.label_kr}({current_state.value}) -> "
            f"{target_state.label_kr}({target_state.value}) 전이가 허용되지 않습니다."
        )


# ---------------------------------------------------------------------------
# Callback type
# ---------------------------------------------------------------------------

# callback(order_id: str, old_state: OrderState, new_state: OrderState, order: dict)
StateChangeCallback = Callable[[str, OrderState, OrderState, Dict[str, Any]], None]


# ---------------------------------------------------------------------------
# OrderStateMachine
# ---------------------------------------------------------------------------


class OrderStateMachine:
    """
    주문 상태 머신.

    모든 주문의 상태를 추적하고, 유효한 전이만 허용하며,
    상태 변경 시 콜백을 통해 외부 컴포넌트에 알림을 전달한다.

    Usage::

        sm = OrderStateMachine()
        sm.register_callback(my_callback)
        order = sm.create_order("ORD-001", {"stock_code": "005930", ...})
        sm.transition("ORD-001", OrderState.SUBMITTED)
    """

    def __init__(self) -> None:
        self._orders: Dict[str, Dict[str, Any]] = {}
        self._callbacks: List[StateChangeCallback] = []

    # ------------------------------------------------------------------
    # Order creation
    # ------------------------------------------------------------------

    def create_order(
        self,
        order_id: str,
        order_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        새 주문을 CREATED 상태로 생성한다.

        Args:
            order_id: 고유 주문 식별자.
            order_data: 종목코드, 수량, 가격 등 주문 정보 딕셔너리.

        Returns:
            생성된 주문 딕셔너리.

        Raises:
            ValueError: 이미 동일한 order_id가 존재하는 경우.
        """
        if order_id in self._orders:
            raise ValueError(f"주문 ID 중복: {order_id}")

        now = time.time()
        order: Dict[str, Any] = {
            "order_id": order_id,
            "state": OrderState.CREATED,
            "created_at": now,
            "updated_at": now,
            "history": [
                {
                    "state": OrderState.CREATED.value,
                    "timestamp": now,
                    "metadata": None,
                },
            ],
            **order_data,
        }
        self._orders[order_id] = order

        logger.info(
            "order_created",
            order_id=order_id,
            state=OrderState.CREATED.value,
            stock_code=order_data.get("stock_code"),
            order_type=order_data.get("order_type"),
            quantity=order_data.get("quantity"),
            price=order_data.get("price"),
        )
        return order

    # ------------------------------------------------------------------
    # State transition
    # ------------------------------------------------------------------

    def transition(
        self,
        order_id: str,
        new_state: OrderState,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        주문 상태를 전이한다.

        1. 유효한 전이인지 검증한다.
        2. 상태를 변경하고 이력에 기록한다.
        3. 등록된 콜백을 실행한다.
        4. FILLED / CANCELLED / EXPIRED 상태이면 on_order_completed 콜백을 추가 실행한다.

        Args:
            order_id: 주문 식별자.
            new_state: 전이할 대상 상태.
            metadata: 추가 메타데이터 (체결가, 체결수량, 사유 등).

        Returns:
            갱신된 주문 딕셔너리.

        Raises:
            KeyError: 해당 order_id가 존재하지 않는 경우.
            InvalidStateTransition: 유효하지 않은 상태 전이인 경우.
        """
        order = self._orders.get(order_id)
        if order is None:
            raise KeyError(f"주문을 찾을 수 없습니다: {order_id}")

        old_state: OrderState = order["state"]

        # Terminal state check
        if old_state in _TERMINAL_STATES:
            raise InvalidStateTransition(order_id, old_state, new_state)

        # Valid transition check
        allowed = VALID_TRANSITIONS.get(old_state)
        if allowed is None or new_state not in allowed:
            raise InvalidStateTransition(order_id, old_state, new_state)

        # Perform transition
        now = time.time()
        order["state"] = new_state
        order["updated_at"] = now
        order["history"].append({
            "state": new_state.value,
            "timestamp": now,
            "metadata": metadata,
        })

        # Merge metadata into order for easy access
        if metadata:
            for key, value in metadata.items():
                if key not in ("state", "order_id", "history", "created_at"):
                    order[key] = value

        logger.info(
            "order_state_transition",
            order_id=order_id,
            old_state=old_state.value,
            old_state_kr=old_state.label_kr,
            new_state=new_state.value,
            new_state_kr=new_state.label_kr,
            stock_code=order.get("stock_code"),
            metadata=metadata,
        )

        # Fire state change callbacks
        for callback in self._callbacks:
            try:
                callback(order_id, old_state, new_state, order)
            except Exception:
                logger.exception(
                    "state_change_callback_error",
                    order_id=order_id,
                    callback=getattr(callback, "__name__", str(callback)),
                )

        # Fire on_order_completed for terminal-completed states
        if new_state in _COMPLETED_STATES:
            self._fire_on_order_completed(order_id, new_state, order)

        return order

    # ------------------------------------------------------------------
    # Completed callback
    # ------------------------------------------------------------------

    def _fire_on_order_completed(
        self,
        order_id: str,
        final_state: OrderState,
        order: Dict[str, Any],
    ) -> None:
        """FILLED / CANCELLED / EXPIRED 상태 전이 시 완료 알림을 발행한다."""
        logger.info(
            "order_completed",
            order_id=order_id,
            final_state=final_state.value,
            final_state_kr=final_state.label_kr,
            stock_code=order.get("stock_code"),
            order_type=order.get("order_type"),
            quantity=order.get("quantity"),
        )

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def get_pending_orders(self) -> List[Dict[str, Any]]:
        """
        미체결 주문 목록을 반환한다.

        SUBMITTED 또는 PARTIAL_FILLED 상태인 주문만 포함한다.

        Returns:
            미체결 주문 딕셔너리 리스트.
        """
        pending_states = {OrderState.SUBMITTED, OrderState.PARTIAL_FILLED}
        return [
            order for order in self._orders.values()
            if order["state"] in pending_states
        ]

    def get_order(self, order_id: str) -> Dict[str, Any]:
        """
        주문 정보를 조회한다.

        Args:
            order_id: 주문 식별자.

        Returns:
            주문 딕셔너리.

        Raises:
            KeyError: 해당 order_id가 존재하지 않는 경우.
        """
        order = self._orders.get(order_id)
        if order is None:
            raise KeyError(f"주문을 찾을 수 없습니다: {order_id}")
        return order

    def get_orders_by_state(self, state: OrderState) -> List[Dict[str, Any]]:
        """특정 상태의 주문 목록을 반환한다."""
        return [
            order for order in self._orders.values()
            if order["state"] == state
        ]

    def get_all_orders(self) -> Dict[str, Dict[str, Any]]:
        """등록된 모든 주문의 복사본을 반환한다."""
        return dict(self._orders)

    # ------------------------------------------------------------------
    # Callback registration
    # ------------------------------------------------------------------

    def register_callback(self, callback: StateChangeCallback) -> None:
        """
        상태 변경 콜백을 등록한다.

        콜백 시그니처:
            callback(order_id: str, old_state: OrderState, new_state: OrderState, order: dict)

        Args:
            callback: 상태 변경 시 호출될 함수.
        """
        self._callbacks.append(callback)
        logger.debug(
            "state_change_callback_registered",
            callback=getattr(callback, "__name__", str(callback)),
            total_callbacks=len(self._callbacks),
        )

    def unregister_callback(self, callback: StateChangeCallback) -> None:
        """등록된 콜백을 제거한다."""
        try:
            self._callbacks.remove(callback)
        except ValueError:
            logger.warning(
                "callback_not_found_for_removal",
                callback=getattr(callback, "__name__", str(callback)),
            )

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def generate_order_id(prefix: str = "ORD") -> str:
        """고유 주문 ID를 생성한다. 형식: ``{prefix}-{epoch_ms}-{uuid4_short}``."""
        epoch_ms = int(time.time() * 1000)
        short_uuid = uuid.uuid4().hex[:8]
        return f"{prefix}-{epoch_ms}-{short_uuid}"

    @property
    def order_count(self) -> int:
        """등록된 전체 주문 수."""
        return len(self._orders)

    def __repr__(self) -> str:
        pending = len(self.get_pending_orders())
        return (
            f"OrderStateMachine(total={self.order_count}, "
            f"pending={pending})"
        )
