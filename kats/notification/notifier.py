"""
KATS Notification Service

Multi-channel notification dispatcher for the Korean Auto-Trading System.
Sends alerts, trade notifications, risk warnings, and daily reports via
Slack (webhook) and Telegram (Bot API).

Usage:
    notifier = NotificationService(
        slack_webhook_url="https://hooks.slack.com/services/T.../B.../xxx",
        telegram_bot_token="123456:ABC-DEF...",
        telegram_chat_id="987654321",
    )
    await notifier.send_trade_notification(trade, r_multiple=1.5)
    await notifier.send_risk_alert("ORANGE", "일일 손실 한도 80% 도달")
    await notifier.send_critical("시스템 긴급 오류 발생!")
"""

from __future__ import annotations

import json
from datetime import datetime
from enum import IntEnum, unique
from typing import Any, Dict, Optional

import aiohttp

from kats.utils.logger import get_logger

logger = get_logger(__name__)


# ============================================================================
# Priority Levels
# ============================================================================

@unique
class Priority(IntEnum):
    """Notification priority levels.

    Higher numeric value = higher urgency.
    """
    INFO = 1
    WARNING = 2
    CRITICAL = 3


# ============================================================================
# Telegram API Constants
# ============================================================================

_TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/sendMessage"


# ============================================================================
# NotificationService
# ============================================================================

class NotificationService:
    """Asynchronous multi-channel notification dispatcher.

    At least one channel (Slack or Telegram) should be configured.
    Unconfigured channels are silently skipped.

    Parameters
    ----------
    slack_webhook_url:
        Slack Incoming Webhook URL.  Leave empty to disable Slack.
    telegram_bot_token:
        Telegram Bot API token.  Leave empty to disable Telegram.
    telegram_chat_id:
        Telegram chat / group ID for message delivery.
    """

    def __init__(
        self,
        slack_webhook_url: str = "",
        telegram_bot_token: str = "",
        telegram_chat_id: str = "",
    ) -> None:
        self._slack_webhook_url: str = slack_webhook_url.strip()
        self._telegram_bot_token: str = telegram_bot_token.strip()
        self._telegram_chat_id: str = telegram_chat_id.strip()
        self._session: Optional[aiohttp.ClientSession] = None

    # ── Session Management ───────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazily create and return a shared ``aiohttp.ClientSession``."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        """Close the underlying HTTP session. Call on shutdown."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ── High-Level Notification Methods ──────────────────────────────────

    async def send_trade_notification(
        self,
        trade: Any,
        r_multiple: Optional[float] = None,
    ) -> None:
        """Send a trade execution notification.

        Args:
            trade: A trade object or dict with ``stock_code``,
                ``order_type``, ``quantity``, ``entry_price``/``exit_price``,
                ``pnl_amount``, ``pnl_percent`` attributes/keys.
            r_multiple: Achieved R-multiple, if available.
        """
        # Support both attribute and dict access
        get = (
            (lambda k, d=None: getattr(trade, k, d))
            if hasattr(trade, "__dict__")
            else (lambda k, d=None: trade.get(k, d))
        )

        stock_code = get("stock_code", "------")
        order_type = get("order_type", "?")
        quantity = get("quantity", 0)
        entry_price = get("entry_price")
        exit_price = get("exit_price")
        pnl_amount = get("pnl_amount")
        pnl_pct = get("pnl_percent")

        side_emoji_map = {"BUY": "BUY", "SELL": "SELL"}
        side_label = side_emoji_map.get(order_type, order_type)

        lines = [
            f"[{side_label}] {stock_code}",
            f"수량: {quantity:,}주",
        ]
        if entry_price is not None:
            lines.append(f"진입가: {entry_price:,.0f}원")
        if exit_price is not None:
            lines.append(f"청산가: {exit_price:,.0f}원")
        if pnl_amount is not None:
            sign = "+" if pnl_amount >= 0 else ""
            lines.append(f"손익: {sign}{pnl_amount:,.0f}원")
        if pnl_pct is not None:
            sign = "+" if pnl_pct >= 0 else ""
            lines.append(f"수익률: {sign}{pnl_pct:.2f}%")
        if r_multiple is not None:
            lines.append(f"R배수: {r_multiple:+.2f}R")

        message = "\n".join(lines)
        priority = Priority.WARNING if (pnl_amount and pnl_amount < 0) else Priority.INFO
        await self._send(message, priority)

    async def send_risk_alert(self, level: str, message: str) -> None:
        """Send a risk management alert.

        Args:
            level: Risk level string (e.g. ``"YELLOW"``, ``"ORANGE"``,
                ``"RED"``, ``"BLACK"``).
            message: Detailed alert message.
        """
        priority_map = {
            "GREEN": Priority.INFO,
            "YELLOW": Priority.WARNING,
            "ORANGE": Priority.WARNING,
            "RED": Priority.CRITICAL,
            "BLACK": Priority.CRITICAL,
        }
        priority = priority_map.get(level.upper(), Priority.WARNING)

        formatted = f"[RISK {level.upper()}]\n{message}"
        await self._send(formatted, priority)

    async def send_event_alert(self, event: Any) -> None:
        """Send a market event / calendar alert.

        Args:
            event: An event object or dict with ``event_name``,
                ``event_date``, ``event_type``, ``market_impact``,
                ``trading_action`` attributes/keys.
        """
        get = (
            (lambda k, d=None: getattr(event, k, d))
            if hasattr(event, "__dict__")
            else (lambda k, d=None: event.get(k, d))
        )

        name = get("event_name", "이벤트")
        event_date = get("event_date", "")
        event_type = get("event_type", "")
        impact = get("market_impact", "")
        action = get("trading_action", "")

        lines = [
            f"[EVENT] {name}",
            f"일자: {event_date}",
            f"유형: {event_type}",
        ]
        if impact:
            lines.append(f"영향도: {impact}")
        if action:
            lines.append(f"매매 조치: {action}")

        message = "\n".join(lines)
        priority = Priority.WARNING if impact == "HIGH" else Priority.INFO
        await self._send(message, priority)

    async def send_daily_report(self, report: Any) -> None:
        """Send the end-of-day performance report.

        Args:
            report: A report object or dict with summary fields such as
                ``total_trades``, ``win_count``, ``loss_count``,
                ``daily_pnl``, ``daily_pnl_pct``, ``max_drawdown``.
        """
        get = (
            (lambda k, d=None: getattr(report, k, d))
            if hasattr(report, "__dict__")
            else (lambda k, d=None: report.get(k, d))
        )

        total = get("total_trades", 0)
        wins = get("win_count", 0)
        losses = get("loss_count", 0)
        pnl = get("daily_pnl", 0.0)
        pnl_pct = get("daily_pnl_pct", 0.0)
        dd = get("max_drawdown", 0.0)

        win_rate = (wins / total * 100) if total > 0 else 0.0
        sign = "+" if pnl >= 0 else ""

        lines = [
            f"[DAILY REPORT] {datetime.now().strftime('%Y-%m-%d')}",
            f"총 거래: {total}건 (승: {wins} / 패: {losses})",
            f"승률: {win_rate:.1f}%",
            f"일일 손익: {sign}{pnl:,.0f}원 ({sign}{pnl_pct:.2f}%)",
            f"최대 드로다운: {dd:.2f}%",
        ]

        message = "\n".join(lines)
        await self._send(message, Priority.INFO)

    async def send_critical(self, message: str) -> None:
        """Send a critical system alert on all configured channels.

        Args:
            message: Critical alert message.
        """
        formatted = f"[CRITICAL]\n{message}"
        await self._send(formatted, Priority.CRITICAL)

    # ── Channel Dispatchers ──────────────────────────────────────────────

    async def _send_slack(self, message: str) -> None:
        """Post a message to Slack via Incoming Webhook.

        Args:
            message: Plain text message body.
        """
        if not self._slack_webhook_url:
            return

        session = await self._get_session()
        payload = {"text": message}

        try:
            async with session.post(
                self._slack_webhook_url,
                json=payload,
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(
                        "slack_send_failed",
                        status=resp.status,
                        body=body[:200],
                    )
                else:
                    logger.debug("slack_send_ok")
        except Exception:
            logger.exception("slack_send_error")

    async def _send_telegram(self, message: str) -> None:
        """Send a message via Telegram Bot API.

        Args:
            message: Plain text message body.
        """
        if not self._telegram_bot_token or not self._telegram_chat_id:
            return

        session = await self._get_session()
        url = _TELEGRAM_API_BASE.format(token=self._telegram_bot_token)
        payload = {
            "chat_id": self._telegram_chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(
                        "telegram_send_failed",
                        status=resp.status,
                        body=body[:200],
                    )
                else:
                    logger.debug("telegram_send_ok")
        except Exception:
            logger.exception("telegram_send_error")

    async def _send(self, message: str, priority: Priority) -> None:
        """Route a message to all configured notification channels.

        For ``CRITICAL`` priority, messages are sent to every channel
        regardless of individual channel preferences.

        Args:
            message: Formatted message string.
            priority: Message priority level.
        """
        logger.info(
            "notification_dispatch",
            priority=priority.name,
            message_preview=message[:100],
        )

        # Send to all configured channels concurrently
        tasks = []

        if self._slack_webhook_url:
            tasks.append(self._send_slack(message))

        if self._telegram_bot_token and self._telegram_chat_id:
            tasks.append(self._send_telegram(message))

        if not tasks:
            logger.warning(
                "notification_no_channels",
                message="알림 채널이 설정되지 않았습니다.",
            )
            return

        # Fire all channels concurrently; individual errors are logged
        # inside each channel method
        import asyncio
        await asyncio.gather(*tasks, return_exceptions=True)
