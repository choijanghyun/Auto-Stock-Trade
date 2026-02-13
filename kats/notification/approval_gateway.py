"""
KATS Approval Gateway (v1.1)

Interactive approval workflow for trading commands that require human
confirmation before execution.  When the MCP handler identifies a
command that needs approval (buy, sell, stop-loss change, etc.), this
gateway:

  1. Sends a formatted approval request via the NotificationService
     with approve / reject action buttons (or text commands).
  2. Tracks the pending approval in an in-memory dict.
  3. Auto-rejects the command if no response is received within the
     configured timeout.

The MCP handler listens for callbacks via ``on_response`` and routes
them to ``MCPHandler.on_approval_received``.

Usage:
    gateway = ApprovalGateway(notifier, timeout=300)
    result = await gateway.request_approval(command)
    # ... later, from a webhook callback ...
    await gateway.on_response(command_id, approved=True)
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Callable, Coroutine, Dict, Optional

from kats.utils.logger import get_logger

logger = get_logger(__name__)

# Default timeout in seconds
_DEFAULT_TIMEOUT_SEC: int = 300


class ApprovalGateway:
    """Manages the approval lifecycle for trading commands.

    Parameters
    ----------
    notifier:
        A ``NotificationService`` instance used to deliver the approval
        request message to the user.
    timeout:
        Maximum seconds to wait for a user response before auto-rejecting.
        Defaults to 300 (5 minutes).
    """

    def __init__(
        self,
        notifier: Any,
        timeout: int = _DEFAULT_TIMEOUT_SEC,
    ) -> None:
        self._notifier = notifier
        self._timeout = timeout

        # command_id -> pending approval state
        self._pending: Dict[str, Dict[str, Any]] = {}

        # Optional callback for when an approval is resolved
        # Signature: async (command_id: str, approved: bool) -> dict
        self._on_resolved: Optional[
            Callable[[str, bool], Coroutine[Any, Any, Dict[str, Any]]]
        ] = None

    # ── Configuration ────────────────────────────────────────────────────

    def set_on_resolved(
        self,
        callback: Callable[[str, bool], Coroutine[Any, Any, Dict[str, Any]]],
    ) -> None:
        """Register the callback invoked when an approval is resolved.

        Typically this is ``MCPHandler.on_approval_received``.

        Args:
            callback: An async callable ``(command_id, approved) -> result``.
        """
        self._on_resolved = callback

    # ── Public API ───────────────────────────────────────────────────────

    async def request_approval(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """Send an interactive approval request to the user.

        The command dictionary must contain at least:
        - ``command_id`` (str)
        - ``intent`` (str)
        - ``summary`` (str)  -- human-readable description of the action

        Args:
            command: Parsed command dictionary from the MCP handler.

        Returns:
            A dict with ``status``, ``command_id``, and ``message`` keys.
        """
        command_id: str = command["command_id"]
        intent: str = command.get("intent", "unknown")
        summary: str = command.get("summary", "")

        logger.info(
            "approval_requested",
            command_id=command_id,
            intent=intent,
        )

        # Build the interactive message
        message = self._build_approval_message(command_id, summary)

        # Start the timeout handler
        timeout_task = asyncio.create_task(
            self._timeout_handler(command_id, self._timeout)
        )

        # Track the pending approval
        self._pending[command_id] = {
            "command": command,
            "created_at": datetime.now(),
            "timeout_task": timeout_task,
            "resolved": False,
        }

        # Deliver the message through the notification service
        try:
            await self._notifier.send_critical(message)
        except Exception:
            logger.exception(
                "approval_notification_failed",
                command_id=command_id,
            )

        return {
            "status": "pending",
            "command_id": command_id,
            "message": f"승인 요청이 전송되었습니다 (제한시간: {self._timeout}초).",
        }

    async def on_response(
        self,
        command_id: str,
        approved: bool,
    ) -> Dict[str, Any]:
        """Handle a user's approve / reject response.

        If the command has already been resolved (e.g. timed out), this
        returns an error status.

        Args:
            command_id: The unique command identifier.
            approved: ``True`` for approval, ``False`` for rejection.

        Returns:
            Result dictionary from the resolved callback, or an error dict.
        """
        pending = self._pending.get(command_id)

        if pending is None:
            logger.warning(
                "approval_response_unknown",
                command_id=command_id,
            )
            return {
                "status": "error",
                "command_id": command_id,
                "message": "해당 승인 요청을 찾을 수 없습니다 (만료 또는 이미 처리됨).",
            }

        if pending["resolved"]:
            return {
                "status": "error",
                "command_id": command_id,
                "message": "이미 처리된 승인 요청입니다.",
            }

        # Mark as resolved and cancel the timeout
        pending["resolved"] = True
        timeout_task: Optional[asyncio.Task[None]] = pending.get("timeout_task")
        if timeout_task and not timeout_task.done():
            timeout_task.cancel()

        # Remove from pending
        self._pending.pop(command_id, None)

        action_label = "승인" if approved else "거부"
        logger.info(
            "approval_resolved",
            command_id=command_id,
            action=action_label,
        )

        # Notify the user of the decision
        try:
            await self._notifier.send_critical(
                f"[{action_label}] 명령 {command_id}\n"
                f"결과: {action_label}되었습니다."
            )
        except Exception:
            logger.exception(
                "approval_resolution_notify_failed",
                command_id=command_id,
            )

        # Invoke the MCP handler callback if registered
        if self._on_resolved:
            return await self._on_resolved(command_id, approved)

        return {
            "status": "resolved",
            "command_id": command_id,
            "approved": approved,
            "message": f"명령이 {action_label}되었습니다.",
        }

    @property
    def pending_count(self) -> int:
        """Number of currently pending approval requests."""
        return len(self._pending)

    def get_pending(self, command_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a pending approval entry by command ID.

        Returns:
            The pending entry dict, or ``None`` if not found.
        """
        return self._pending.get(command_id)

    # ── Timeout Handler ──────────────────────────────────────────────────

    async def _timeout_handler(self, command_id: str, timeout: int) -> None:
        """Auto-reject a pending approval after the timeout elapses.

        Args:
            command_id: The command being timed.
            timeout: Seconds to wait.
        """
        try:
            await asyncio.sleep(timeout)
        except asyncio.CancelledError:
            return

        pending = self._pending.get(command_id)
        if pending is None or pending["resolved"]:
            return

        logger.warning(
            "approval_timeout",
            command_id=command_id,
            timeout_sec=timeout,
        )

        pending["resolved"] = True
        self._pending.pop(command_id, None)

        # Notify the user
        try:
            await self._notifier.send_critical(
                f"[TIMEOUT] 명령 {command_id}\n"
                f"승인 시간이 초과되어 자동 거부되었습니다 ({timeout}초)."
            )
        except Exception:
            logger.exception(
                "approval_timeout_notify_failed",
                command_id=command_id,
            )

        # Invoke the MCP handler callback with rejected=False
        if self._on_resolved:
            try:
                await self._on_resolved(command_id, False)
            except Exception:
                logger.exception(
                    "approval_timeout_callback_failed",
                    command_id=command_id,
                )

    # ── Message Formatting ───────────────────────────────────────────────

    @staticmethod
    def _build_approval_message(command_id: str, summary: str) -> str:
        """Build a formatted approval request message with action hints.

        Args:
            command_id: Unique command identifier.
            summary: Human-readable command summary.

        Returns:
            Formatted message string.
        """
        lines = [
            "[APPROVAL REQUIRED]",
            f"명령 ID: {command_id}",
            "",
            summary,
            "",
            "응답 방법:",
            f"  승인: /approve {command_id}",
            f"  거부: /reject {command_id}",
            "",
            "* 응답이 없으면 자동 거부됩니다.",
        ]
        return "\n".join(lines)
