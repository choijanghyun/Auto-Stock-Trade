"""
통합 주문 관리자 (Order Manager).

전략 시그널을 수신하여 리스크 검증, 주문 생성, 실전/모의 라우팅,
상태 추적, 피라미딩을 통합 관리하는 최상위 주문 파사드(Facade).

모든 주문 흐름은 반드시 OrderManager를 경유해야 하며,
직접 REST Client나 Paper Engine을 호출하지 않는다.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional, Protocol

import structlog

from kats.api.kis_rest_client import KISRestClient
from kats.order.order_state_machine import (
    InvalidStateTransition,
    OrderState,
    OrderStateMachine,
)
from kats.order.order_tracker import OrderTracker
from kats.order.paper_trading import PaperTradingEngine
from kats.order.pyramid_manager import PyramidManager

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Protocol interfaces for loosely-coupled dependencies
# ---------------------------------------------------------------------------


class RiskManagerProtocol(Protocol):
    """RiskManager에 요구되는 인터페이스."""

    def validate_order(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        주문 리스크를 검증한다.

        Returns:
            {"approved": bool, "reason": str, "adjusted_quantity": int, ...}
        """
        ...

    def on_order_filled(self, fill_data: Dict[str, Any]) -> None:
        """체결 완료 시 리스크 상태를 갱신한다."""
        ...


class MarginGuardProtocol(Protocol):
    """MarginGuard에 요구되는 인터페이스."""

    def check_margin(
        self,
        order_type: str,
        amount: float,
        locked_capital: float,
    ) -> Dict[str, Any]:
        """
        증거금/예수금 여유를 확인한다.

        Returns:
            {"sufficient": bool, "available": float, "required": float, ...}
        """
        ...


class TradeSignal(Protocol):
    """전략 시그널에 요구되는 인터페이스.

    dict로도 사용 가능하지만, 구조적 타이핑을 위해 Protocol로 정의한다.
    """

    stock_code: str
    order_type: str       # "BUY" | "SELL"
    quantity: int
    price: float
    strategy_code: str
    stop_loss_price: float
    confidence: int       # 1~5


# ---------------------------------------------------------------------------
# OrderManager
# ---------------------------------------------------------------------------


class OrderManager:
    """
    통합 주문 관리자.

    전략 시그널 -> 리스크 검증 -> 주문 생성 -> 실전/모의 라우팅의
    전체 주문 생명주기를 관리한다.

    구성 요소:
    - KISRestClient: 실전 주문 REST API 통신
    - OrderStateMachine: 주문 상태 FSM
    - OrderTracker: 미체결 주문 추적 및 자동 취소
    - PaperTradingEngine: 모의투자 체결 시뮬레이션
    - RiskManager: 리스크 검증 (포지션 한도, 일일 손실 등)
    - MarginGuard: 증거금/예수금 확인
    - PyramidManager: 피라미딩 단계 관리

    Usage::

        om = OrderManager(
            rest_client=rest_client,
            state_machine=state_machine,
            order_tracker=tracker,
            paper_engine=paper_engine,
            risk_manager=risk_mgr,
            margin_guard=margin_guard,
            pyramid_manager=pyramid_mgr,
            trade_mode="PAPER",
        )
        result = await om.place_order(signal)
    """

    def __init__(
        self,
        rest_client: KISRestClient,
        state_machine: OrderStateMachine,
        order_tracker: OrderTracker,
        paper_engine: PaperTradingEngine,
        risk_manager: RiskManagerProtocol,
        margin_guard: MarginGuardProtocol,
        pyramid_manager: PyramidManager,
        trade_mode: str = "PAPER",
    ) -> None:
        self._rest_client = rest_client
        self._state_machine = state_machine
        self._order_tracker = order_tracker
        self._paper_engine = paper_engine
        self._risk_manager = risk_manager
        self._margin_guard = margin_guard
        self._pyramid_manager = pyramid_manager
        self._trade_mode = trade_mode.upper()

        # 신규 주문 차단 플래그 (일일 손실 한도 도달 등)
        self._block_new_orders: bool = False

        # 오픈 포지션 추적 (in-memory)
        self._open_positions: Dict[str, Dict[str, Any]] = {}

        # 체결 콜백 등록
        self._state_machine.register_callback(self._on_state_change)

        logger.info(
            "order_manager_initialized",
            trade_mode=self._trade_mode,
            components={
                "rest_client": type(rest_client).__name__,
                "state_machine": type(state_machine).__name__,
                "order_tracker": type(order_tracker).__name__,
                "paper_engine": type(paper_engine).__name__,
                "risk_manager": type(risk_manager).__name__,
                "margin_guard": type(margin_guard).__name__,
                "pyramid_manager": type(pyramid_manager).__name__,
            },
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def block_new_orders(self) -> bool:
        """신규 주문 차단 여부."""
        return self._block_new_orders

    @block_new_orders.setter
    def block_new_orders(self, value: bool) -> None:
        """신규 주문 차단 플래그를 설정한다."""
        old = self._block_new_orders
        self._block_new_orders = value
        if old != value:
            logger.warning(
                "order_block_flag_changed",
                old=old,
                new=value,
                msg="신규 주문 차단됨" if value else "신규 주문 허용",
            )

    @property
    def trade_mode(self) -> str:
        """현재 매매 모드 (LIVE / PAPER)."""
        return self._trade_mode

    # ------------------------------------------------------------------
    # Place order
    # ------------------------------------------------------------------

    async def place_order(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        전략 시그널을 기반으로 주문을 실행한다.

        처리 흐름:
        1. 차단 플래그 확인
        2. RiskManager 검증
        3. MarginGuard 증거금 확인
        4. OrderStateMachine에 주문 생성 (CREATED)
        5. 실전(LIVE) 또는 모의(PAPER) 엔진으로 라우팅
        6. 결과에 따라 상태 전이 (SUBMITTED -> FILLED 등)

        Args:
            signal: 전략 시그널 딕셔너리.
                Required keys:
                - stock_code, order_type, quantity, price
                - strategy_code, stop_loss_price, confidence

        Returns:
            주문 결과 딕셔너리:
            - success: bool
            - order_id: 주문 식별자
            - state: 현재 주문 상태
            - fill_data: 체결 정보 (체결 시)
            - error: 오류 메시지 (실패 시)
        """
        stock_code: str = signal.get("stock_code", "")
        order_type: str = signal.get("order_type", "")
        quantity: int = int(signal.get("quantity", 0))
        price: float = float(signal.get("price", 0))
        strategy_code: str = signal.get("strategy_code", "")

        logger.info(
            "place_order_request",
            stock_code=stock_code,
            order_type=order_type,
            quantity=quantity,
            price=price,
            strategy=strategy_code,
            trade_mode=self._trade_mode,
        )

        # 1) 차단 플래그 확인
        if self._block_new_orders:
            logger.warning(
                "order_blocked",
                stock_code=stock_code,
                order_type=order_type,
                reason="신규 주문이 차단되어 있습니다.",
            )
            return {
                "success": False,
                "order_id": None,
                "state": None,
                "fill_data": None,
                "error": "신규 주문이 차단되어 있습니다 (block_new_orders=True).",
            }

        # 2) RiskManager 검증
        risk_result = self._risk_manager.validate_order(signal)
        if not risk_result.get("approved", False):
            reason = risk_result.get("reason", "리스크 검증 실패")
            logger.warning(
                "order_risk_rejected",
                stock_code=stock_code,
                order_type=order_type,
                reason=reason,
            )
            return {
                "success": False,
                "order_id": None,
                "state": None,
                "fill_data": None,
                "error": f"리스크 검증 거부: {reason}",
                "risk_result": risk_result,
            }

        # 리스크 관리자가 수량을 조정했을 수 있음
        adjusted_qty = int(risk_result.get("adjusted_quantity", quantity))
        if adjusted_qty != quantity:
            logger.info(
                "order_quantity_adjusted",
                original=quantity,
                adjusted=adjusted_qty,
                reason=risk_result.get("adjustment_reason", ""),
            )
            quantity = adjusted_qty

        # 3) MarginGuard 증거금 확인 (매수 주문만)
        if order_type == "BUY":
            locked_capital = self._order_tracker.get_locked_capital()
            order_amount = price * quantity
            margin_result = self._margin_guard.check_margin(
                order_type=order_type,
                amount=order_amount,
                locked_capital=locked_capital,
            )
            if not margin_result.get("sufficient", False):
                logger.warning(
                    "order_margin_insufficient",
                    stock_code=stock_code,
                    required=margin_result.get("required", 0),
                    available=margin_result.get("available", 0),
                )
                return {
                    "success": False,
                    "order_id": None,
                    "state": None,
                    "fill_data": None,
                    "error": "증거금 부족",
                    "margin_result": margin_result,
                }

        # 4) 주문 ID 생성 및 주문 데이터 구성
        order_id = OrderStateMachine.generate_order_id()
        order_data: Dict[str, Any] = {
            "stock_code": stock_code,
            "order_type": order_type,
            "quantity": quantity,
            "price": price,
            "strategy_code": strategy_code,
            "stop_loss_price": signal.get("stop_loss_price", 0),
            "confidence": signal.get("confidence", 0),
            "trade_mode": self._trade_mode,
            "filled_quantity": 0,
            "fill_price": 0.0,
        }

        # 상태 머신에 주문 생성 (CREATED)
        try:
            self._state_machine.create_order(order_id, order_data)
        except ValueError as exc:
            logger.error("order_create_failed", error=str(exc))
            return {
                "success": False,
                "order_id": order_id,
                "state": None,
                "fill_data": None,
                "error": str(exc),
            }

        # 5) 실전/모의 라우팅
        try:
            if self._trade_mode == "LIVE":
                result = await self._execute_live_order(order_id, order_data)
            else:
                result = await self._execute_paper_order(order_id, order_data)
        except Exception as exc:
            logger.exception(
                "order_execution_error",
                order_id=order_id,
                stock_code=stock_code,
            )
            try:
                self._state_machine.transition(
                    order_id,
                    OrderState.ERROR,
                    metadata={"error": str(exc)},
                )
            except InvalidStateTransition:
                pass
            return {
                "success": False,
                "order_id": order_id,
                "state": OrderState.ERROR.value,
                "fill_data": None,
                "error": str(exc),
            }

        return result

    # ------------------------------------------------------------------
    # Live order execution
    # ------------------------------------------------------------------

    async def _execute_live_order(
        self,
        order_id: str,
        order_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        실전 주문을 KIS REST API로 실행한다.

        CREATED -> SUBMITTED 전이 후 REST 주문을 전송한다.
        체결 결과는 WebSocket 체결 알림(OrderTracker)으로 비동기 수신된다.

        Args:
            order_id: 주문 식별자.
            order_data: 주문 데이터.

        Returns:
            주문 접수 결과 딕셔너리.
        """
        stock_code = order_data["stock_code"]
        order_type = order_data["order_type"]
        quantity = order_data["quantity"]
        price = int(order_data["price"])

        # CREATED -> SUBMITTED
        self._state_machine.transition(
            order_id,
            OrderState.SUBMITTED,
            metadata={"submitted_via": "kis_rest_api"},
        )

        # REST API 주문 실행
        api_result = await self._rest_client.place_order(
            stock_code=stock_code,
            order_type=order_type,
            quantity=quantity,
            price=price,
        )

        # KIS 응답에서 주문번호 추출
        output = api_result.get("output", {})
        broker_order_no = output.get("ODNO", "")
        order_time = output.get("ORD_TMD", "")

        # 주문번호를 상태 머신 주문에 기록
        order = self._state_machine.get_order(order_id)
        order["broker_order_no"] = broker_order_no
        order["order_time"] = order_time

        logger.info(
            "live_order_submitted",
            order_id=order_id,
            broker_order_no=broker_order_no,
            stock_code=stock_code,
            order_type=order_type,
            quantity=quantity,
            price=price,
            order_time=order_time,
        )

        return {
            "success": True,
            "order_id": order_id,
            "state": OrderState.SUBMITTED.value,
            "broker_order_no": broker_order_no,
            "fill_data": None,  # 체결은 WebSocket으로 비동기 수신
            "error": None,
            "trade_mode": "LIVE",
        }

    # ------------------------------------------------------------------
    # Paper order execution
    # ------------------------------------------------------------------

    async def _execute_paper_order(
        self,
        order_id: str,
        order_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        모의 주문을 PaperTradingEngine으로 실행한다.

        CREATED -> SUBMITTED -> FILLED/PARTIAL_FILLED 전이를 동기적으로 처리한다.

        Args:
            order_id: 주문 식별자.
            order_data: 주문 데이터.

        Returns:
            체결 결과 딕셔너리.
        """
        stock_code = order_data["stock_code"]

        # CREATED -> SUBMITTED
        self._state_machine.transition(
            order_id,
            OrderState.SUBMITTED,
            metadata={"submitted_via": "paper_engine"},
        )

        # 가상 체결 실행
        fill_result = self._paper_engine.execute_virtual_order(order_data)

        if not fill_result.get("success", False):
            # 체결 실패 -> REJECTED
            self._state_machine.transition(
                order_id,
                OrderState.REJECTED,
                metadata={
                    "reject_reason": fill_result.get("error", "모의 체결 실패"),
                },
            )
            return {
                "success": False,
                "order_id": order_id,
                "state": OrderState.REJECTED.value,
                "fill_data": fill_result,
                "error": fill_result.get("error", "모의 체결 실패"),
                "trade_mode": "PAPER",
            }

        fill_type = fill_result.get("fill_type", "")
        fill_price = fill_result.get("fill_price", 0)
        fill_quantity = fill_result.get("fill_quantity", 0)
        remaining_quantity = fill_result.get("remaining_quantity", 0)

        if remaining_quantity > 0:
            # 부분 체결
            new_state = OrderState.PARTIAL_FILLED
        else:
            # 전량 체결
            new_state = OrderState.FILLED

        self._state_machine.transition(
            order_id,
            new_state,
            metadata={
                "fill_price": fill_price,
                "filled_quantity": fill_quantity,
                "remaining_quantity": remaining_quantity,
                "fill_type": fill_type,
                "slippage_pct": fill_result.get("slippage_pct", 0),
                "market_impact_pct": fill_result.get("market_impact_pct", 0),
            },
        )

        # 체결 콜백 처리
        if new_state == OrderState.FILLED:
            self.on_order_fill(order_id, fill_result)

        # 오픈 포지션 갱신
        if order_data["order_type"] == "BUY":
            self._update_open_position(order_id, order_data, fill_result)

        logger.info(
            "paper_order_executed",
            order_id=order_id,
            stock_code=stock_code,
            state=new_state.value,
            fill_type=fill_type,
            fill_price=fill_price,
            fill_quantity=fill_quantity,
            remaining_quantity=remaining_quantity,
        )

        return {
            "success": True,
            "order_id": order_id,
            "state": new_state.value,
            "fill_data": fill_result,
            "error": None,
            "trade_mode": "PAPER",
        }

    # ------------------------------------------------------------------
    # Cancel order
    # ------------------------------------------------------------------

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        주문을 취소한다.

        Args:
            order_id: 취소할 주문의 식별자.

        Returns:
            취소 결과 딕셔너리.
        """
        try:
            order = self._state_machine.get_order(order_id)
        except KeyError:
            return {
                "success": False,
                "order_id": order_id,
                "error": f"주문을 찾을 수 없습니다: {order_id}",
            }

        current_state: OrderState = order["state"]
        stock_code = order.get("stock_code", "")

        logger.info(
            "cancel_order_request",
            order_id=order_id,
            stock_code=stock_code,
            current_state=current_state.value,
        )

        try:
            # CANCEL_REQUESTED 전이
            self._state_machine.transition(
                order_id,
                OrderState.CANCEL_REQUESTED,
                metadata={"cancel_reason": "사용자 요청"},
            )

            # 실전 모드이면 REST API 취소
            if self._trade_mode == "LIVE":
                broker_order_no = order.get("broker_order_no", "")
                if broker_order_no:
                    await self._rest_client.cancel_order(
                        order_no=broker_order_no,
                        stock_code=stock_code,
                    )

            # CANCELLED 전이
            self._state_machine.transition(
                order_id,
                OrderState.CANCELLED,
                metadata={"cancelled_by": "user"},
            )

            logger.info(
                "order_cancelled",
                order_id=order_id,
                stock_code=stock_code,
            )

            return {
                "success": True,
                "order_id": order_id,
                "state": OrderState.CANCELLED.value,
                "error": None,
            }

        except InvalidStateTransition as exc:
            logger.warning(
                "cancel_order_transition_failed",
                order_id=order_id,
                error=str(exc),
            )
            return {
                "success": False,
                "order_id": order_id,
                "error": str(exc),
            }
        except Exception as exc:
            logger.exception(
                "cancel_order_failed",
                order_id=order_id,
            )
            return {
                "success": False,
                "order_id": order_id,
                "error": str(exc),
            }

    # ------------------------------------------------------------------
    # Cancel all pending
    # ------------------------------------------------------------------

    async def cancel_all_pending(self) -> List[Dict[str, Any]]:
        """
        모든 미체결 주문을 일괄 취소한다.

        Returns:
            각 주문의 취소 결과 리스트.
        """
        pending = self._state_machine.get_pending_orders()
        results: List[Dict[str, Any]] = []

        logger.info(
            "cancel_all_pending_start",
            pending_count=len(pending),
        )

        for order in pending:
            order_id = order["order_id"]
            result = await self.cancel_order(order_id)
            results.append(result)

        cancelled_count = sum(1 for r in results if r.get("success", False))
        logger.info(
            "cancel_all_pending_complete",
            total=len(pending),
            cancelled=cancelled_count,
            failed=len(pending) - cancelled_count,
        )

        return results

    # ------------------------------------------------------------------
    # Close all positions
    # ------------------------------------------------------------------

    async def close_all_positions(self) -> List[Dict[str, Any]]:
        """
        모든 오픈 포지션을 청산한다.

        각 포지션에 대해 시장가 매도 주문을 생성한다.

        Returns:
            각 포지션의 청산 결과 리스트.
        """
        results: List[Dict[str, Any]] = []
        positions = list(self._open_positions.values())

        logger.info(
            "close_all_positions_start",
            position_count=len(positions),
        )

        for position in positions:
            stock_code = position.get("stock_code", "")
            quantity = position.get("quantity", 0)

            if quantity <= 0:
                continue

            sell_signal: Dict[str, Any] = {
                "stock_code": stock_code,
                "order_type": "SELL",
                "quantity": quantity,
                "price": 0,  # 시장가
                "strategy_code": position.get("strategy_code", ""),
                "stop_loss_price": 0,
                "confidence": 0,
            }

            result = await self.place_order(sell_signal)
            results.append(result)

        closed_count = sum(1 for r in results if r.get("success", False))
        logger.info(
            "close_all_positions_complete",
            total=len(positions),
            closed=closed_count,
            failed=len(positions) - closed_count,
        )

        return results

    # ------------------------------------------------------------------
    # Modify order
    # ------------------------------------------------------------------

    async def modify_order(
        self,
        order_id: str,
        new_price: float,
    ) -> Dict[str, Any]:
        """
        주문 가격을 정정한다.

        Args:
            order_id: 정정할 주문의 식별자.
            new_price: 새로운 주문 가격.

        Returns:
            정정 결과 딕셔너리.
        """
        try:
            order = self._state_machine.get_order(order_id)
        except KeyError:
            return {
                "success": False,
                "order_id": order_id,
                "error": f"주문을 찾을 수 없습니다: {order_id}",
            }

        current_state: OrderState = order["state"]
        stock_code = order.get("stock_code", "")

        logger.info(
            "modify_order_request",
            order_id=order_id,
            stock_code=stock_code,
            current_state=current_state.value,
            old_price=order.get("price"),
            new_price=new_price,
        )

        # 부분 체결 상태에서만 AMEND_REQUESTED 전이 가능
        # SUBMITTED 상태에서는 직접 REST 정정 후 상태 유지
        try:
            if current_state == OrderState.PARTIAL_FILLED:
                self._state_machine.transition(
                    order_id,
                    OrderState.AMEND_REQUESTED,
                    metadata={"new_price": new_price, "amend_reason": "사용자 가격 정정"},
                )

            # 실전 모드: REST API 정정
            if self._trade_mode == "LIVE":
                broker_order_no = order.get("broker_order_no", "")
                if broker_order_no:
                    await self._rest_client.modify_order(
                        order_no=broker_order_no,
                        stock_code=stock_code,
                        new_price=int(new_price),
                    )

            # 정정 후 SUBMITTED 복귀 (AMEND_REQUESTED -> SUBMITTED)
            if current_state == OrderState.PARTIAL_FILLED:
                self._state_machine.transition(
                    order_id,
                    OrderState.SUBMITTED,
                    metadata={"amend_result": "가격 정정 완료", "new_price": new_price},
                )
            else:
                # SUBMITTED 상태에서는 가격만 갱신
                order["price"] = new_price
                order["updated_at"] = time.time()

            logger.info(
                "order_modified",
                order_id=order_id,
                stock_code=stock_code,
                new_price=new_price,
            )

            return {
                "success": True,
                "order_id": order_id,
                "new_price": new_price,
                "state": self._state_machine.get_order(order_id)["state"].value,
                "error": None,
            }

        except InvalidStateTransition as exc:
            logger.warning(
                "modify_order_transition_failed",
                order_id=order_id,
                error=str(exc),
            )
            return {
                "success": False,
                "order_id": order_id,
                "error": str(exc),
            }
        except Exception as exc:
            logger.exception(
                "modify_order_failed",
                order_id=order_id,
            )
            return {
                "success": False,
                "order_id": order_id,
                "error": str(exc),
            }

    # ------------------------------------------------------------------
    # Open positions
    # ------------------------------------------------------------------

    def get_open_positions(self) -> List[Dict[str, Any]]:
        """
        오픈 포지션 목록을 반환한다.

        Returns:
            보유 수량이 0보다 큰 포지션 딕셔너리 리스트.
        """
        return [
            pos for pos in self._open_positions.values()
            if pos.get("quantity", 0) > 0
        ]

    def _update_open_position(
        self,
        order_id: str,
        order_data: Dict[str, Any],
        fill_result: Dict[str, Any],
    ) -> None:
        """오픈 포지션을 갱신한다 (매수 체결 시)."""
        stock_code = order_data["stock_code"]
        fill_price = fill_result.get("fill_price", 0)
        fill_quantity = fill_result.get("fill_quantity", 0)

        existing = self._open_positions.get(stock_code)
        if existing is not None:
            # 기존 포지션에 추가 (피라미딩 등)
            old_qty = existing["quantity"]
            old_cost = existing.get("total_cost", existing["avg_entry_price"] * old_qty)
            new_qty = old_qty + fill_quantity
            new_cost = old_cost + (fill_price * fill_quantity)
            existing["quantity"] = new_qty
            existing["avg_entry_price"] = new_cost / new_qty if new_qty > 0 else 0
            existing["total_cost"] = new_cost
            existing["updated_at"] = time.time()
        else:
            self._open_positions[stock_code] = {
                "stock_code": stock_code,
                "order_id": order_id,
                "quantity": fill_quantity,
                "avg_entry_price": fill_price,
                "total_cost": fill_price * fill_quantity,
                "strategy_code": order_data.get("strategy_code", ""),
                "stop_loss_price": order_data.get("stop_loss_price", 0),
                "trade_mode": self._trade_mode,
                "entry_time": time.time(),
                "updated_at": time.time(),
            }

    def _reduce_open_position(
        self,
        stock_code: str,
        sell_quantity: int,
        sell_price: float,
    ) -> None:
        """오픈 포지션을 감소시킨다 (매도 체결 시)."""
        position = self._open_positions.get(stock_code)
        if position is None:
            return

        position["quantity"] -= sell_quantity
        if position["quantity"] <= 0:
            # 포지션 청산 완료
            del self._open_positions[stock_code]
            logger.info(
                "position_closed",
                stock_code=stock_code,
                sell_price=sell_price,
            )
        else:
            position["total_cost"] = position["avg_entry_price"] * position["quantity"]
            position["updated_at"] = time.time()

    # ------------------------------------------------------------------
    # Fill callback
    # ------------------------------------------------------------------

    def on_order_fill(
        self,
        order_id: str,
        fill_data: Dict[str, Any],
    ) -> None:
        """
        주문 체결 콜백.

        체결 완료 시 호출되어 리스크 매니저에 통보하고,
        피라미딩 기회를 확인한다.

        Args:
            order_id: 체결된 주문 식별자.
            fill_data: 체결 데이터 (fill_price, fill_quantity 등).
        """
        try:
            order = self._state_machine.get_order(order_id)
        except KeyError:
            logger.warning(
                "fill_callback_unknown_order",
                order_id=order_id,
            )
            return

        stock_code = order.get("stock_code", "")
        order_type = order.get("order_type", "")
        fill_price = fill_data.get("fill_price", 0)
        fill_quantity = fill_data.get("fill_quantity", 0)

        logger.info(
            "on_order_fill",
            order_id=order_id,
            stock_code=stock_code,
            order_type=order_type,
            fill_price=fill_price,
            fill_quantity=fill_quantity,
        )

        # 리스크 매니저에 통보
        try:
            self._risk_manager.on_order_filled({
                "order_id": order_id,
                "stock_code": stock_code,
                "order_type": order_type,
                "fill_price": fill_price,
                "fill_quantity": fill_quantity,
                "strategy_code": order.get("strategy_code", ""),
                "trade_mode": self._trade_mode,
            })
        except Exception:
            logger.exception(
                "risk_manager_fill_notification_error",
                order_id=order_id,
            )

        # 매도 시 포지션 감소
        if order_type == "SELL":
            self._reduce_open_position(stock_code, fill_quantity, fill_price)

    # ------------------------------------------------------------------
    # State change callback (registered on state machine)
    # ------------------------------------------------------------------

    def _on_state_change(
        self,
        order_id: str,
        old_state: OrderState,
        new_state: OrderState,
        order: Dict[str, Any],
    ) -> None:
        """상태 머신 콜백 -- 내부 로깅 및 정리용."""
        logger.debug(
            "order_manager_state_change",
            order_id=order_id,
            old_state=old_state.value,
            new_state=new_state.value,
            stock_code=order.get("stock_code"),
        )

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        pending = len(self._state_machine.get_pending_orders())
        positions = len(self.get_open_positions())
        return (
            f"OrderManager(mode={self._trade_mode}, "
            f"pending={pending}, positions={positions}, "
            f"blocked={self._block_new_orders})"
        )
