"""
KATS MCP (Model Context Protocol) Command Handler

Orchestrates the full lifecycle of a natural-language trading command:

  1. Parse intent + entities via NLPParser
  2. Validate the parsed result
  3. For *immediate* intents (status, report) -- execute directly
  4. For *approval-required* intents (buy, sell, stop-loss, …) --
     send a formatted summary to the ApprovalGateway and wait

When the user approves (or the timeout expires), ``on_approval_received``
completes or rejects the command.

Usage:
    handler = MCPHandler(nlp_parser, approval_gateway, order_manager, perf)
    result = await handler.process_command("삼성전자 5% 오르면 100주 매수")
    # ... later, from the approval callback ...
    result = await handler.on_approval_received(cmd_id, approved=True)
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from kats.utils.logger import get_logger

logger = get_logger(__name__)


# ============================================================================
# Intent Metadata
# ============================================================================

SUPPORTED_INTENTS: Dict[str, str] = {
    "conditional_buy":    "조건부 매수",
    "conditional_sell":   "조건부 매도",
    "modify_stop_loss":   "손절가 변경",
    "performance_report": "성과 보고",
    "status_inquiry":     "상태 조회",
    "strategy_setup":     "전략 설정",
    "cancel_order":       "주문 취소",
}

IMMEDIATE_INTENTS: frozenset[str] = frozenset({
    "status_inquiry",
    "performance_report",
})

APPROVAL_REQUIRED_INTENTS: frozenset[str] = frozenset({
    "conditional_buy",
    "conditional_sell",
    "modify_stop_loss",
    "strategy_setup",
    "cancel_order",
})

APPROVAL_TIMEOUT_SEC: int = 300  # 5 minutes


# ============================================================================
# MCPHandler
# ============================================================================

class MCPHandler:
    """Central command handler for the MCP natural-language pipeline.

    Parameters
    ----------
    nlp_parser:
        An ``NLPParser`` instance used for intent classification and entity
        extraction.
    approval_gateway:
        An ``ApprovalGateway`` instance that sends interactive approval
        requests and tracks their state.
    order_manager:
        The order management component that can execute buy/sell/cancel
        actions.  Expected interface: ``submit_conditional_order``,
        ``modify_stop_loss``, ``cancel_order``, ``get_positions``.
    performance_analyzer:
        The performance / reporting component.  Expected interface:
        ``generate_report``, ``get_status``.
    """

    def __init__(
        self,
        nlp_parser: Any,
        approval_gateway: Any,
        order_manager: Any,
        performance_analyzer: Any,
    ) -> None:
        self._parser = nlp_parser
        self._approval = approval_gateway
        self._orders = order_manager
        self._performance = performance_analyzer

        # command_id -> {intent, entities, created_at, timeout_task}
        self._pending_commands: Dict[str, Dict[str, Any]] = {}

    # ── Public API ───────────────────────────────────────────────────────

    async def process_command(self, natural_language: str) -> Dict[str, Any]:
        """Parse and process a natural-language trading command.

        Args:
            natural_language: Raw Korean text from the user.

        Returns:
            A result dictionary with at least ``status`` and ``message`` keys.
        """
        command_id = uuid.uuid4().hex[:12]
        logger.info(
            "mcp_command_received",
            command_id=command_id,
            text=natural_language[:120],
        )

        # ── 1. Parse ─────────────────────────────────────────────────────
        intent = self._parser.parse_intent(natural_language)
        entities = self._parser.extract_entities(natural_language)

        logger.info(
            "mcp_command_parsed",
            command_id=command_id,
            intent=intent,
            entities=entities,
        )

        # ── 2. Validate ─────────────────────────────────────────────────
        validation = self._validate(intent, entities)
        if not validation["valid"]:
            logger.warning(
                "mcp_validation_failed",
                command_id=command_id,
                reason=validation["reason"],
            )
            return {
                "status": "error",
                "command_id": command_id,
                "message": validation["reason"],
            }

        # ── 3. Route ─────────────────────────────────────────────────────
        if intent in IMMEDIATE_INTENTS:
            return await self._execute_command(command_id, intent, entities)

        if intent in APPROVAL_REQUIRED_INTENTS:
            return await self._request_approval(command_id, intent, entities)

        return {
            "status": "error",
            "command_id": command_id,
            "message": "알 수 없는 명령입니다. 다시 입력해 주세요.",
        }

    async def on_approval_received(
        self,
        command_id: str,
        approved: bool,
    ) -> Dict[str, Any]:
        """Handle an approval or rejection callback for a pending command.

        Args:
            command_id: The unique identifier returned in the initial
                ``process_command`` response.
            approved: ``True`` if the user approved the command.

        Returns:
            Execution result or rejection acknowledgement.
        """
        pending = self._pending_commands.pop(command_id, None)
        if pending is None:
            logger.warning(
                "mcp_approval_unknown_command",
                command_id=command_id,
            )
            return {
                "status": "error",
                "command_id": command_id,
                "message": "해당 명령을 찾을 수 없습니다 (만료 또는 이미 처리됨).",
            }

        # Cancel the timeout task if still running
        timeout_task: Optional[asyncio.Task[None]] = pending.get("timeout_task")
        if timeout_task and not timeout_task.done():
            timeout_task.cancel()

        if approved:
            logger.info("mcp_command_approved", command_id=command_id)
            return await self._execute_command(
                command_id,
                pending["intent"],
                pending["entities"],
            )

        logger.info("mcp_command_rejected", command_id=command_id)
        return {
            "status": "rejected",
            "command_id": command_id,
            "message": "사용자에 의해 거부되었습니다.",
        }

    # ── Formatting ───────────────────────────────────────────────────────

    @staticmethod
    def _format_command_for_human(
        intent: str,
        entities: Dict[str, Any],
    ) -> str:
        """Build a Korean-readable summary of a parsed command.

        Args:
            intent: Classified intent string.
            entities: Extracted entities dictionary.

        Returns:
            Formatted Korean string suitable for display in a chat message.
        """
        intent_label = SUPPORTED_INTENTS.get(intent, intent)
        stock_name = entities.get("stock_name", "종목 미지정")
        stock_code = entities.get("stock_code", "")

        parts = [f"[{intent_label}]"]

        if stock_name:
            code_suffix = f" ({stock_code})" if stock_code else ""
            parts.append(f"종목: {stock_name}{code_suffix}")

        direction = entities.get("direction")
        threshold = entities.get("threshold")
        price = entities.get("price")

        if threshold is not None and direction:
            dir_kr = "상승" if direction == "up" else "하락"
            parts.append(f"조건: {threshold}% {dir_kr}")
        elif price is not None and direction:
            dir_kr = "이상" if direction == "up" else "이하"
            parts.append(f"조건: {price:,}원 {dir_kr}")
        elif price is not None:
            parts.append(f"가격: {price:,}원")

        quantity = entities.get("quantity")
        if quantity is not None:
            if quantity == -1:
                parts.append("수량: 전량")
            else:
                parts.append(f"수량: {quantity:,}주")

        return "\n".join(parts)

    # ── Validation ───────────────────────────────────────────────────────

    @staticmethod
    def _validate(
        intent: str,
        entities: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Validate parsed intent and entities for completeness.

        Returns:
            ``{"valid": True}`` or ``{"valid": False, "reason": "..."}``
        """
        if intent == "unknown":
            return {
                "valid": False,
                "reason": (
                    "명령을 이해할 수 없습니다. "
                    "예: '삼성전자 5% 오르면 100주 매수해줘'"
                ),
            }

        # Buy/sell require a stock
        if intent in ("conditional_buy", "conditional_sell"):
            if "stock_code" not in entities:
                return {
                    "valid": False,
                    "reason": "종목을 인식할 수 없습니다. 종목명을 정확히 입력해 주세요.",
                }

        # modify_stop_loss requires stock + price
        if intent == "modify_stop_loss":
            if "stock_code" not in entities:
                return {
                    "valid": False,
                    "reason": "손절가를 변경할 종목을 입력해 주세요.",
                }
            if "price" not in entities:
                return {
                    "valid": False,
                    "reason": "새로운 손절가(원)를 입력해 주세요.",
                }

        # cancel_order requires stock
        if intent == "cancel_order":
            if "stock_code" not in entities:
                return {
                    "valid": False,
                    "reason": "주문 취소할 종목을 입력해 주세요.",
                }

        return {"valid": True}

    # ── Approval Flow ────────────────────────────────────────────────────

    async def _request_approval(
        self,
        command_id: str,
        intent: str,
        entities: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Send an approval request and register the pending command.

        Returns:
            A dictionary with ``status="awaiting_approval"`` and the
            formatted command summary.
        """
        summary = self._format_command_for_human(intent, entities)

        # Store in pending commands
        timeout_task = asyncio.create_task(
            self._timeout_handler(command_id, APPROVAL_TIMEOUT_SEC)
        )
        self._pending_commands[command_id] = {
            "intent": intent,
            "entities": entities,
            "summary": summary,
            "created_at": datetime.now(),
            "timeout_task": timeout_task,
        }

        # Send the approval request via the gateway
        try:
            await self._approval.request_approval({
                "command_id": command_id,
                "intent": intent,
                "entities": entities,
                "summary": summary,
            })
        except Exception:
            logger.exception(
                "mcp_approval_send_failed",
                command_id=command_id,
            )

        logger.info(
            "mcp_awaiting_approval",
            command_id=command_id,
            intent=intent,
        )
        return {
            "status": "awaiting_approval",
            "command_id": command_id,
            "intent": intent,
            "summary": summary,
            "message": (
                f"승인 대기 중입니다 (제한시간 {APPROVAL_TIMEOUT_SEC}초).\n\n{summary}"
            ),
        }

    async def _timeout_handler(self, command_id: str, timeout: int) -> None:
        """Auto-reject a pending command after the timeout elapses."""
        try:
            await asyncio.sleep(timeout)
        except asyncio.CancelledError:
            return

        pending = self._pending_commands.pop(command_id, None)
        if pending is not None:
            logger.warning(
                "mcp_command_timeout",
                command_id=command_id,
                intent=pending["intent"],
            )
            # Notify the user that the command timed out
            try:
                await self._approval.request_approval({
                    "command_id": command_id,
                    "intent": "timeout",
                    "summary": (
                        f"명령이 시간 초과되어 자동 거부되었습니다.\n\n"
                        f"{pending['summary']}"
                    ),
                })
            except Exception:
                logger.exception(
                    "mcp_timeout_notification_failed",
                    command_id=command_id,
                )

    # ── Command Execution ────────────────────────────────────────────────

    async def _execute_command(
        self,
        command_id: str,
        intent: str,
        entities: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Dispatch an approved / immediate command to the appropriate handler.

        Returns:
            Execution result dictionary.
        """
        logger.info(
            "mcp_executing_command",
            command_id=command_id,
            intent=intent,
        )

        try:
            if intent == "status_inquiry":
                data = await self._performance.get_status()
                return {
                    "status": "success",
                    "command_id": command_id,
                    "intent": intent,
                    "data": data,
                    "message": "현재 포지션 상태입니다.",
                }

            if intent == "performance_report":
                data = await self._performance.generate_report()
                return {
                    "status": "success",
                    "command_id": command_id,
                    "intent": intent,
                    "data": data,
                    "message": "성과 보고서입니다.",
                }

            if intent == "conditional_buy":
                result = await self._orders.submit_conditional_order(
                    side="BUY",
                    stock_code=entities["stock_code"],
                    quantity=entities.get("quantity"),
                    threshold_pct=entities.get("threshold"),
                    trigger_price=entities.get("price"),
                    direction=entities.get("direction", "up"),
                )
                return {
                    "status": "success",
                    "command_id": command_id,
                    "intent": intent,
                    "data": result,
                    "message": "조건부 매수 주문이 등록되었습니다.",
                }

            if intent == "conditional_sell":
                result = await self._orders.submit_conditional_order(
                    side="SELL",
                    stock_code=entities["stock_code"],
                    quantity=entities.get("quantity"),
                    threshold_pct=entities.get("threshold"),
                    trigger_price=entities.get("price"),
                    direction=entities.get("direction", "down"),
                )
                return {
                    "status": "success",
                    "command_id": command_id,
                    "intent": intent,
                    "data": result,
                    "message": "조건부 매도 주문이 등록되었습니다.",
                }

            if intent == "modify_stop_loss":
                result = await self._orders.modify_stop_loss(
                    stock_code=entities["stock_code"],
                    new_price=entities["price"],
                )
                return {
                    "status": "success",
                    "command_id": command_id,
                    "intent": intent,
                    "data": result,
                    "message": (
                        f"손절가가 {entities['price']:,}원으로 변경되었습니다."
                    ),
                }

            if intent == "strategy_setup":
                result = await self._orders.setup_strategy(
                    stock_code=entities.get("stock_code"),
                    entities=entities,
                )
                return {
                    "status": "success",
                    "command_id": command_id,
                    "intent": intent,
                    "data": result,
                    "message": "전략이 설정되었습니다.",
                }

            if intent == "cancel_order":
                result = await self._orders.cancel_order(
                    stock_code=entities["stock_code"],
                )
                return {
                    "status": "success",
                    "command_id": command_id,
                    "intent": intent,
                    "data": result,
                    "message": "주문이 취소되었습니다.",
                }

            return {
                "status": "error",
                "command_id": command_id,
                "message": f"지원되지 않는 명령입니다: {intent}",
            }

        except Exception as exc:
            logger.exception(
                "mcp_execution_failed",
                command_id=command_id,
                intent=intent,
                error=str(exc),
            )
            return {
                "status": "error",
                "command_id": command_id,
                "intent": intent,
                "message": f"명령 실행 중 오류가 발생했습니다: {exc}",
            }
