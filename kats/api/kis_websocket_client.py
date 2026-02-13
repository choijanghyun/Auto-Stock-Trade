"""
KIS WebSocket 실시간 데이터 수신 클라이언트.

체결가, 호가, VI(변동성 완화장치) 정보, 주문 체결 통보를
실시간으로 수신하며, 자동 재연결과 구독 복원을 처리한다.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
import websockets
import websockets.exceptions

logger = structlog.get_logger(__name__)

# 콜백 타입: 파싱된 데이터 딕셔너리를 받는 비동기 함수
MessageCallback = Callable[[dict[str, Any]], Awaitable[None]]


class KISWebSocketClient:
    """
    KIS WebSocket 실시간 데이터 클라이언트.

    동작 원리:
        - ``connect()`` 호출 시 WebSocket 연결을 수립하고 메시지 수신 루프에 진입.
        - 연결이 끊기면 지수 백오프로 자동 재연결 (최대 30회).
        - 재연결 시 기존 구독을 자동 복원.
        - PINGPONG 하트비트에 자동 응답하여 연결 유지.

    메시지 형식:
        - 실시간 데이터: ``"0|H0STCNT0|001|..."`` (파이프 구분)
          첫 문자 ``0`` 또는 ``1`` = 암호화 여부, 이후 tr_id, 건수, 데이터.
        - JSON 응답: 구독 확인, 에러 등.

    Args:
        approval_key: WebSocket 접속키 (REST ``/oauth2/Approval`` 으로 발급).
    """

    WS_URL: str = "ws://ops.koreainvestment.com:21000"

    # 구독 tr_id 상수
    TR_EXECUTION: str = "H0STCNT0"    # 실시간 체결
    TR_ORDERBOOK: str = "H0STASP0"    # 실시간 호가
    TR_VI: str = "H0STVI0"            # VI 발동/해제
    TR_ORDER_NOTICE: str = "H0STCNC0"  # 주문 체결 통보

    _MAX_RECONNECTS: int = 30
    _MAX_BACKOFF_SECONDS: float = 60.0

    def __init__(self, approval_key: str) -> None:
        self._approval_key = approval_key
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._subscriptions: dict[str, set[str]] = {}  # {tr_id: {stock_codes}}
        self._callbacks: dict[str, MessageCallback] = {}
        self._reconnect_count: int = 0
        self._running: bool = False

        logger.info("kis_websocket_client_initialized")

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """
        WebSocket 연결을 수립하고 메시지 수신 루프에 진입한다.

        연결이 끊기면 지수 백오프(최대 60초)로 자동 재연결한다.
        최대 재연결 횟수(30회)를 초과하면 치명적 오류를 기록하고 종료한다.
        """
        self._running = True

        while self._running and self._reconnect_count < self._MAX_RECONNECTS:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    self._reconnect_count = 0
                    logger.info("kis_websocket_connected", url=self.WS_URL)

                    # 기존 구독 복원
                    await self._restore_subscriptions()

                    # 메시지 수신 루프
                    async for message in ws:
                        await self._handle_message(message)

            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                ConnectionError,
                OSError,
            ) as exc:
                self._ws = None

                if not self._running:
                    logger.info("kis_websocket_shutdown_requested")
                    break

                self._reconnect_count += 1
                wait = min(
                    2 ** self._reconnect_count,
                    self._MAX_BACKOFF_SECONDS,
                )

                logger.warning(
                    "kis_websocket_disconnected",
                    error=str(exc),
                    reconnect_attempt=self._reconnect_count,
                    max_reconnects=self._MAX_RECONNECTS,
                    wait_seconds=wait,
                )
                await asyncio.sleep(wait)

            except asyncio.CancelledError:
                logger.info("kis_websocket_cancelled")
                self._running = False
                break

        if self._reconnect_count >= self._MAX_RECONNECTS:
            logger.critical(
                "kis_websocket_max_reconnects_exceeded",
                reconnect_count=self._reconnect_count,
                message="WebSocket 최대 재연결 횟수 초과. 시스템 점검 필요.",
            )

    async def disconnect(self) -> None:
        """WebSocket 연결을 정상 종료한다."""
        self._running = False

        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning("kis_websocket_close_error", error=str(exc))
            finally:
                self._ws = None

        logger.info("kis_websocket_disconnected_gracefully")

    # ------------------------------------------------------------------
    # Subscription helpers
    # ------------------------------------------------------------------

    async def _subscribe(self, tr_id: str, tr_key: str) -> None:
        """
        구독 요청 메시지를 전송한다.

        Args:
            tr_id: 구독 거래 ID.
            tr_key: 구독 키 (종목코드 또는 빈 문자열).
        """
        if self._ws is None:
            logger.error(
                "kis_websocket_not_connected",
                action="subscribe",
                tr_id=tr_id,
                tr_key=tr_key,
            )
            return

        msg: dict[str, Any] = {
            "header": {
                "approval_key": self._approval_key,
                "custtype": "P",
                "tr_type": "1",  # 1=등록, 2=해제
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": tr_key,
                },
            },
        }

        await self._ws.send(json.dumps(msg))
        self._subscriptions.setdefault(tr_id, set()).add(tr_key)

        logger.info(
            "kis_websocket_subscribed",
            tr_id=tr_id,
            tr_key=tr_key,
        )

    async def _unsubscribe(self, tr_id: str, tr_key: str) -> None:
        """
        구독 해제 메시지를 전송한다.

        Args:
            tr_id: 구독 거래 ID.
            tr_key: 구독 키 (종목코드 또는 빈 문자열).
        """
        if self._ws is None:
            return

        msg: dict[str, Any] = {
            "header": {
                "approval_key": self._approval_key,
                "custtype": "P",
                "tr_type": "2",  # 2=해제
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": tr_key,
                },
            },
        }

        await self._ws.send(json.dumps(msg))

        if tr_id in self._subscriptions:
            self._subscriptions[tr_id].discard(tr_key)
            if not self._subscriptions[tr_id]:
                del self._subscriptions[tr_id]

        logger.info(
            "kis_websocket_unsubscribed",
            tr_id=tr_id,
            tr_key=tr_key,
        )

    async def _restore_subscriptions(self) -> None:
        """재연결 후 기존 구독을 모두 복원한다."""
        total = sum(len(codes) for codes in self._subscriptions.values())
        if total == 0:
            return

        for tr_id, keys in list(self._subscriptions.items()):
            for key in list(keys):
                await self._subscribe(tr_id, key)
                # 구독 복원 시 서버 부하 방지를 위해 짧은 대기
                await asyncio.sleep(0.1)

        logger.info(
            "kis_websocket_subscriptions_restored",
            total_subscriptions=total,
        )

    # ------------------------------------------------------------------
    # Public subscription methods
    # ------------------------------------------------------------------

    async def subscribe_execution(self, stock_code: str) -> None:
        """
        실시간 체결 통보를 구독한다 (tr_id: H0STCNT0).

        Args:
            stock_code: 종목코드 6자리.
        """
        await self._subscribe(self.TR_EXECUTION, stock_code)

    async def subscribe_orderbook(self, stock_code: str) -> None:
        """
        실시간 호가 변화를 구독한다 (tr_id: H0STASP0).

        Args:
            stock_code: 종목코드 6자리.
        """
        await self._subscribe(self.TR_ORDERBOOK, stock_code)

    async def subscribe_vi(self, stock_code: str) -> None:
        """
        VI(변동성 완화장치) 발동/해제를 구독한다 (tr_id: H0STVI0).

        Args:
            stock_code: 종목코드 6자리.
        """
        await self._subscribe(self.TR_VI, stock_code)

    async def subscribe_order_notice(self) -> None:
        """
        주문 체결 통보를 구독한다 (tr_id: H0STCNC0).

        개인 주문의 체결/미체결 실시간 알림을 수신한다.
        tr_key는 HTS ID를 사용하나, 빈 문자열로 전체 구독할 수 있다.
        """
        await self._subscribe(self.TR_ORDER_NOTICE, "")

    # ------------------------------------------------------------------
    # Callback registration
    # ------------------------------------------------------------------

    def register_callback(self, tr_id: str, callback: MessageCallback) -> None:
        """
        특정 tr_id에 대한 콜백 함수를 등록한다.

        기존 콜백이 있으면 덮어쓴다.

        Args:
            tr_id: 콜백을 연결할 거래 ID (예: ``"H0STCNT0"``).
            callback: 파싱된 데이터를 인자로 받는 비동기 함수.
        """
        self._callbacks[tr_id] = callback
        logger.info("kis_websocket_callback_registered", tr_id=tr_id)

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    async def _handle_message(self, raw: str) -> None:
        """
        수신 메시지를 파싱하고 등록된 콜백을 실행한다.

        메시지 형식:
            - 실시간 데이터: 첫 문자가 '0' 또는 '1' (암호화 여부).
              ``"0|H0STCNT0|001|005930^..."`` 형태로 파이프(|) 구분.
            - JSON 응답: 구독 확인, 에러, PINGPONG 하트비트.
        """
        if not raw:
            return

        # 실시간 데이터 (파이프 구분 형식)
        if raw[0] in ("0", "1"):
            await self._handle_realtime_data(raw)
            return

        # JSON 응답
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("kis_websocket_invalid_json", raw_preview=raw[:200])
            return

        # PINGPONG 하트비트 응답
        header = data.get("header", {})
        if header.get("tr_id") == "PINGPONG":
            if self._ws is not None:
                await self._ws.send(raw)
                logger.debug("kis_websocket_pingpong_replied")
            return

        # 구독 확인 등 기타 JSON 응답
        tr_id = header.get("tr_id", "")
        msg_cd = data.get("body", {}).get("msg_cd", "")
        msg1 = data.get("body", {}).get("msg1", "")

        logger.debug(
            "kis_websocket_json_response",
            tr_id=tr_id,
            msg_cd=msg_cd,
            msg=msg1,
        )

    async def _handle_realtime_data(self, raw: str) -> None:
        """
        파이프 구분 실시간 데이터를 파싱하고 콜백을 호출한다.

        형식: ``"<encrypted>|<tr_id>|<count>|<data>"``
            - encrypted: ``"0"``=평문, ``"1"``=암호화
            - tr_id: 거래 ID (예: H0STCNT0)
            - count: 데이터 건수
            - data: 실제 데이터 (``^`` 구분 필드)
        """
        parts = raw.split("|")
        if len(parts) < 4:
            logger.warning(
                "kis_websocket_malformed_realtime",
                parts_count=len(parts),
                raw_preview=raw[:200],
            )
            return

        encrypted = parts[0]
        tr_id = parts[1]
        count = parts[2]
        data_raw = parts[3]

        if encrypted == "1":
            # 암호화된 데이터는 AES-CBC 복호화 필요 (향후 구현)
            logger.debug(
                "kis_websocket_encrypted_data",
                tr_id=tr_id,
                message="암호화 데이터 수신. 복호화 로직 미구현.",
            )
            return

        parsed = self._parse_realtime_fields(tr_id, data_raw)
        parsed["_meta"] = {
            "tr_id": tr_id,
            "count": int(count) if count.isdigit() else 0,
            "encrypted": encrypted == "1",
        }

        # 등록된 콜백 실행
        callback = self._callbacks.get(tr_id)
        if callback is not None:
            try:
                await callback(parsed)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "kis_websocket_callback_error",
                    tr_id=tr_id,
                    error=str(exc),
                    exc_info=True,
                )
        else:
            logger.debug(
                "kis_websocket_no_callback",
                tr_id=tr_id,
            )

    @staticmethod
    def _parse_realtime_fields(tr_id: str, data_raw: str) -> dict[str, Any]:
        """
        tr_id별로 ``^`` 구분 필드를 의미 있는 딕셔너리로 변환한다.

        KIS 실시간 데이터는 ``^`` 구분자로 필드가 나열된다.
        각 tr_id별 필드 순서는 KIS 문서를 따른다.
        """
        fields = data_raw.split("^")

        if tr_id == "H0STCNT0":
            # 실시간 체결 (주요 필드 추출)
            return _parse_execution_fields(fields)

        if tr_id == "H0STASP0":
            # 실시간 호가
            return _parse_orderbook_fields(fields)

        if tr_id == "H0STVI0":
            # VI 발동/해제
            return _parse_vi_fields(fields)

        if tr_id == "H0STCNC0":
            # 주문 체결 통보
            return _parse_order_notice_fields(fields)

        # 알 수 없는 tr_id
        return {"raw_fields": fields}

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        """WebSocket이 연결 상태인지 반환한다."""
        return self._ws is not None and self._ws.open

    @property
    def subscription_count(self) -> int:
        """현재 활성 구독 총 건수를 반환한다."""
        return sum(len(keys) for keys in self._subscriptions.values())


# ======================================================================
# 실시간 데이터 필드 파서 (모듈 레벨 함수)
# ======================================================================


def _safe_get(fields: list[str], index: int, default: str = "") -> str:
    """필드 리스트에서 인덱스 안전하게 조회."""
    return fields[index] if index < len(fields) else default


def _parse_execution_fields(fields: list[str]) -> dict[str, Any]:
    """
    실시간 체결(H0STCNT0) 필드를 파싱한다.

    KIS 실시간 체결 데이터의 주요 필드를 추출한다.
    전체 필드 수는 약 46개이며, 매매에 핵심적인 필드를 선별한다.
    """
    return {
        "stock_code": _safe_get(fields, 0),          # 종목코드
        "exec_time": _safe_get(fields, 1),            # 체결시간 (HHMMSS)
        "current_price": _safe_get(fields, 2),        # 현재가
        "change_sign": _safe_get(fields, 3),          # 전일 대비 부호
        "change_amount": _safe_get(fields, 4),        # 전일 대비
        "change_rate": _safe_get(fields, 5),          # 전일 대비율
        "weighted_avg_price": _safe_get(fields, 6),   # 가중 평균가
        "open_price": _safe_get(fields, 7),           # 시가
        "high_price": _safe_get(fields, 8),           # 고가
        "low_price": _safe_get(fields, 9),            # 저가
        "ask_price1": _safe_get(fields, 10),          # 매도호가1
        "bid_price1": _safe_get(fields, 11),          # 매수호가1
        "exec_volume": _safe_get(fields, 12),         # 체결 거래량
        "cumulative_volume": _safe_get(fields, 13),   # 누적 거래량
        "cumulative_amount": _safe_get(fields, 14),   # 누적 거래대금
        "sell_buy_flag": _safe_get(fields, 15),       # 매도/매수 구분 (1=매도, 2=매수)
        "total_sell_volume": _safe_get(fields, 16),   # 총 매도 잔량
        "total_buy_volume": _safe_get(fields, 17),    # 총 매수 잔량
        "exec_strength": _safe_get(fields, 18),       # 체결강도
    }


def _parse_orderbook_fields(fields: list[str]) -> dict[str, Any]:
    """
    실시간 호가(H0STASP0) 필드를 파싱한다.

    10단계 매도/매수 호가와 잔량, 시간 정보를 추출한다.
    """
    result: dict[str, Any] = {
        "stock_code": _safe_get(fields, 0),
        "exec_time": _safe_get(fields, 1),
    }

    # 매도호가 1~10 (인덱스 3~12), 매수호가 1~10 (인덱스 13~22)
    # 매도잔량 1~10 (인덱스 23~32), 매수잔량 1~10 (인덱스 33~42)
    ask_prices: list[str] = []
    bid_prices: list[str] = []
    ask_volumes: list[str] = []
    bid_volumes: list[str] = []

    for i in range(10):
        ask_prices.append(_safe_get(fields, 3 + i))
        bid_prices.append(_safe_get(fields, 13 + i))
        ask_volumes.append(_safe_get(fields, 23 + i))
        bid_volumes.append(_safe_get(fields, 33 + i))

    result["ask_prices"] = ask_prices
    result["bid_prices"] = bid_prices
    result["ask_volumes"] = ask_volumes
    result["bid_volumes"] = bid_volumes
    result["total_ask_volume"] = _safe_get(fields, 43)
    result["total_bid_volume"] = _safe_get(fields, 44)

    return result


def _parse_vi_fields(fields: list[str]) -> dict[str, Any]:
    """
    VI 발동/해제(H0STVI0) 필드를 파싱한다.

    변동성 완화장치 상태 정보를 추출한다.
    """
    return {
        "stock_code": _safe_get(fields, 0),           # 종목코드
        "vi_time": _safe_get(fields, 1),              # VI 발동/해제 시각
        "vi_type": _safe_get(fields, 2),              # VI 구분 (정적/동적/복합)
        "vi_status": _safe_get(fields, 3),            # 상태 (발동/해제)
        "static_vi_base_price": _safe_get(fields, 4), # 정적 VI 기준가
        "dynamic_vi_base_price": _safe_get(fields, 5),# 동적 VI 기준가
        "vi_trigger_price": _safe_get(fields, 6),     # VI 발동 가격
    }


def _parse_order_notice_fields(fields: list[str]) -> dict[str, Any]:
    """
    주문/체결 통보(H0STCNC0) 필드를 파싱한다.

    주문 접수, 체결, 거부 등의 실시간 알림 정보를 추출한다.
    """
    return {
        "order_date": _safe_get(fields, 0),           # 주문일자
        "order_time": _safe_get(fields, 1),           # 주문시각
        "account_no": _safe_get(fields, 2),           # 계좌번호
        "order_no": _safe_get(fields, 3),             # 주문번호
        "stock_code": _safe_get(fields, 4),           # 종목코드
        "order_type": _safe_get(fields, 5),           # 주문구분 (매수/매도)
        "order_dvsn": _safe_get(fields, 6),           # 주문종류 (지정가/시장가)
        "order_price": _safe_get(fields, 7),          # 주문가격
        "order_qty": _safe_get(fields, 8),            # 주문수량
        "exec_price": _safe_get(fields, 9),           # 체결가격
        "exec_qty": _safe_get(fields, 10),            # 체결수량
        "exec_amount": _safe_get(fields, 11),         # 체결금액
        "remaining_qty": _safe_get(fields, 12),       # 미체결수량
        "order_status": _safe_get(fields, 13),        # 주문상태
        "reject_reason": _safe_get(fields, 14),       # 거부사유
    }
