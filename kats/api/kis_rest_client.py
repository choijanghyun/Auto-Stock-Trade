"""
KIS REST API 통신 래퍼 클라이언트.

주문/잔고/시세 조회 등 KIS Developers REST API 호출을 담당한다.
자동 Rate Limiting, 해시키 첨부, 지수 백오프 재시도 로직을 내장한다.
"""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

import aiohttp
import structlog

from kats.api.rate_limiter import RateLimiter

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Protocols for dependency injection (TokenManager, HashkeyManager)
# ---------------------------------------------------------------------------


class TokenManagerProtocol(Protocol):
    """TokenManager에 요구되는 인터페이스."""

    app_key: str
    app_secret: str

    async def get_token(self) -> str: ...


class HashkeyManagerProtocol(Protocol):
    """HashkeyManager에 요구되는 인터페이스."""

    async def get_hashkey(self, body: dict[str, Any]) -> str: ...


# ---------------------------------------------------------------------------
# Custom Exception
# ---------------------------------------------------------------------------


class KISAPIError(Exception):
    """KIS REST API 비정상 응답 예외.

    Attributes:
        msg_cd: KIS 응답 메시지 코드 (예: ``"EGW00123"``).
        msg: KIS 응답 메시지 본문.
        response_data: 원본 응답 딕셔너리 전체.
    """

    def __init__(
        self,
        msg_cd: str | None = None,
        msg: str | None = None,
        response_data: dict[str, Any] | None = None,
    ) -> None:
        self.msg_cd = msg_cd or "UNKNOWN"
        self.msg = msg or "알 수 없는 오류"
        self.response_data = response_data or {}
        super().__init__(f"[{self.msg_cd}] {self.msg}")


# ---------------------------------------------------------------------------
# KISRestClient
# ---------------------------------------------------------------------------

# 재시도 대상 KIS 오류 코드 (일시적 서버 오류)
_RETRYABLE_MSG_CODES: frozenset[str] = frozenset({
    "EGW00200",  # 초당 거래건수 초과
    "EGW00201",  # 일 거래건수 초과 (대기 후 재시도 가능)
})

_MAX_RETRY_ATTEMPTS: int = 3


class KISRestClient:
    """
    KIS REST API 통신 클라이언트.

    모든 API 호출은 ``request()`` 메서드를 경유하며,
    Rate Limiting, 헤더 자동 구성, 해시키 첨부, 지수 백오프 재시도를 처리한다.

    Args:
        token_manager: 접근 토큰 발급/갱신 관리자.
        hashkey_manager: POST 요청용 해시키 생성 관리자.
        rate_limiter: API 호출 속도 제한기.
        mode: ``"LIVE"`` (실전) 또는 ``"PAPER"`` (모의).
        account_no: 계좌번호 앞 8자리 (예: ``"50123456"``).
        account_product_code: 계좌 상품코드 뒤 2자리 (예: ``"01"``).
    """

    BASE_URL_LIVE: str = "https://openapi.koreainvestment.com:9443"
    BASE_URL_PAPER: str = "https://openapivts.koreainvestment.com:29443"

    def __init__(
        self,
        token_manager: TokenManagerProtocol,
        hashkey_manager: HashkeyManagerProtocol,
        rate_limiter: RateLimiter,
        mode: str = "PAPER",
        account_no: str = "",
        account_product_code: str = "01",
    ) -> None:
        self._token_manager = token_manager
        self._hashkey_manager = hashkey_manager
        self._rate_limiter = rate_limiter
        self._mode = mode.upper()
        self._account_no = account_no
        self._account_product_code = account_product_code

        self._base_url: str = (
            self.BASE_URL_LIVE if self._mode == "LIVE" else self.BASE_URL_PAPER
        )
        self._session: aiohttp.ClientSession | None = None

        logger.info(
            "kis_rest_client_initialized",
            mode=self._mode,
            base_url=self._base_url,
            account_no=self._account_no[:4] + "****" if self._account_no else "N/A",
        )

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """aiohttp 세션이 없거나 닫혔으면 새로 생성한다."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        """HTTP 세션을 정리한다. 애플리케이션 종료 시 반드시 호출."""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("kis_rest_client_session_closed")
        self._session = None

    # ------------------------------------------------------------------
    # Core request
    # ------------------------------------------------------------------

    async def request(
        self,
        method: str,
        path: str,
        tr_id: str,
        body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        KIS REST API 요청을 실행한다.

        1. Rate Limiter를 통해 호출 권한 획득
        2. Authorization, appkey, appsecret 등 공통 헤더 구성
        3. POST 요청이면 해시키를 헤더에 첨부
        4. 실패 시 지수 백오프로 최대 3회 재시도

        Args:
            method: HTTP 메서드 (``"GET"`` / ``"POST"``).
            path: API 경로 (예: ``"/uapi/domestic-stock/v1/quotations/inquire-price"``).
            tr_id: 거래 ID (예: ``"FHKST01010100"``).
            body: POST 요청 본문 (JSON).
            params: GET 쿼리 파라미터.

        Returns:
            KIS 응답 JSON 딕셔너리.

        Raises:
            KISAPIError: KIS가 오류 응답을 반환한 경우.
            aiohttp.ClientError: 네트워크 레벨 오류가 재시도 소진 후에도 해소되지 않은 경우.
        """
        session = await self._ensure_session()

        # Rate limit 대기
        await self._rate_limiter.acquire()

        # 공통 헤더 구성
        token = await self._token_manager.get_token()
        headers: dict[str, str] = {
            "authorization": f"Bearer {token}",
            "appkey": self._token_manager.app_key,
            "appsecret": self._token_manager.app_secret,
            "tr_id": tr_id,
            "content-type": "application/json; charset=utf-8",
            "custtype": "P",
        }

        # POST 요청이면 해시키 첨부
        if method.upper() == "POST" and body:
            headers["hashkey"] = await self._hashkey_manager.get_hashkey(body)

        # 지수 백오프 재시도
        last_exception: BaseException | None = None
        for attempt in range(_MAX_RETRY_ATTEMPTS):
            try:
                async with session.request(
                    method,
                    f"{self._base_url}{path}",
                    headers=headers,
                    json=body,
                    params=params,
                ) as resp:
                    data: dict[str, Any] = await resp.json()

                    rt_cd = data.get("rt_cd")
                    if rt_cd != "0":
                        msg_cd = data.get("msg_cd", "")
                        msg1 = data.get("msg1", "")

                        # 일시적 오류이면 재시도
                        if msg_cd in _RETRYABLE_MSG_CODES and attempt < _MAX_RETRY_ATTEMPTS - 1:
                            wait = 2 ** (attempt + 1)
                            logger.warning(
                                "kis_api_retryable_error",
                                msg_cd=msg_cd,
                                msg=msg1,
                                attempt=attempt + 1,
                                wait_seconds=wait,
                            )
                            await asyncio.sleep(wait)
                            continue

                        raise KISAPIError(
                            msg_cd=msg_cd,
                            msg=msg1,
                            response_data=data,
                        )

                    logger.debug(
                        "kis_api_request_success",
                        method=method,
                        path=path,
                        tr_id=tr_id,
                    )
                    return data

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exception = exc
                if attempt < _MAX_RETRY_ATTEMPTS - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        "kis_api_network_error",
                        error=str(exc),
                        attempt=attempt + 1,
                        max_attempts=_MAX_RETRY_ATTEMPTS,
                        wait_seconds=wait,
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(
                        "kis_api_request_failed",
                        error=str(exc),
                        method=method,
                        path=path,
                        tr_id=tr_id,
                        attempts=_MAX_RETRY_ATTEMPTS,
                    )

        # 모든 재시도 실패
        raise last_exception  # type: ignore[misc]

    # ------------------------------------------------------------------
    # 시세 조회 API
    # ------------------------------------------------------------------

    async def get_current_price(self, stock_code: str) -> dict[str, Any]:
        """
        주식 현재가 조회.

        Args:
            stock_code: 종목코드 6자리 (예: ``"005930"``).

        Returns:
            현재가 응답 딕셔너리.
        """
        return await self.request(
            "GET",
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            tr_id="FHKST01010100",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": stock_code,
            },
        )

    async def get_asking_price(self, stock_code: str) -> dict[str, Any]:
        """
        주식 호가 조회.

        Args:
            stock_code: 종목코드 6자리.

        Returns:
            호가 응답 딕셔너리.
        """
        return await self.request(
            "GET",
            "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
            tr_id="FHKST01010200",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": stock_code,
            },
        )

    async def get_daily_price(
        self,
        stock_code: str,
        period: str = "D",
        count: int = 100,
    ) -> dict[str, Any]:
        """
        주식 기간별 시세 (일/주/월봉).

        Args:
            stock_code: 종목코드 6자리.
            period: 기간 구분 (``"D"``=일, ``"W"``=주, ``"M"``=월).
            count: 조회 건수 (참고용, KIS API 자체 페이징 적용).

        Returns:
            기간별 시세 응답 딕셔너리.
        """
        return await self.request(
            "GET",
            "/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            tr_id="FHKST01010400",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": stock_code,
                "FID_PERIOD_DIV_CODE": period,
                "FID_ORG_ADJ_PRC": "0",
            },
        )

    # ------------------------------------------------------------------
    # 주문 API
    # ------------------------------------------------------------------

    def _get_order_tr_id(self, order_type: str) -> str:
        """매매 모드와 주문 유형에 따라 적절한 tr_id를 반환한다."""
        if self._mode == "LIVE":
            return "TTTC0802U" if order_type == "BUY" else "TTTC0801U"
        else:
            return "VTTC0802U" if order_type == "BUY" else "VTTC0801U"

    async def place_order(
        self,
        stock_code: str,
        order_type: str,
        quantity: int,
        price: int = 0,
    ) -> dict[str, Any]:
        """
        주식 매수/매도 주문.

        Args:
            stock_code: 종목코드 6자리.
            order_type: ``"BUY"`` 또는 ``"SELL"``.
            quantity: 주문 수량.
            price: 주문 가격. 0이면 시장가 주문.

        Returns:
            주문 결과 응답 딕셔너리.
        """
        tr_id = self._get_order_tr_id(order_type)

        # 00=지정가, 01=시장가
        order_dvsn = "00" if price > 0 else "01"

        body: dict[str, str] = {
            "CANO": self._account_no,
            "ACNT_PRDT_CD": self._account_product_code,
            "PDNO": stock_code,
            "ORD_DVSN": order_dvsn,
            "ORD_QTY": str(quantity),
            "ORD_UNPR": str(price),
        }

        logger.info(
            "kis_order_request",
            stock_code=stock_code,
            order_type=order_type,
            quantity=quantity,
            price=price,
            tr_id=tr_id,
            order_dvsn=order_dvsn,
        )

        return await self.request(
            "POST",
            "/uapi/domestic-stock/v1/trading/order-cash",
            tr_id=tr_id,
            body=body,
        )

    async def cancel_order(
        self,
        order_no: str,
        stock_code: str,
    ) -> dict[str, Any]:
        """
        주문 취소.

        Args:
            order_no: 원주문번호.
            stock_code: 종목코드 6자리.

        Returns:
            취소 결과 응답 딕셔너리.
        """
        tr_id = "TTTC0803U" if self._mode == "LIVE" else "VTTC0803U"

        body: dict[str, str] = {
            "CANO": self._account_no,
            "ACNT_PRDT_CD": self._account_product_code,
            "KRX_FWDG_ORD_ORGNO": "",
            "ORGN_ODNO": order_no,
            "ORD_DVSN": "00",
            "RVSE_CNCL_DVSN_CD": "02",  # 02=취소
            "ORD_QTY": "0",  # 전량 취소
            "ORD_UNPR": "0",
            "QTY_ALL_ORD_YN": "Y",
        }

        logger.info(
            "kis_cancel_order_request",
            order_no=order_no,
            stock_code=stock_code,
        )

        return await self.request(
            "POST",
            "/uapi/domestic-stock/v1/trading/order-rvsecncl",
            tr_id=tr_id,
            body=body,
        )

    async def modify_order(
        self,
        order_no: str,
        stock_code: str,
        new_price: int,
        order_dvsn: str = "00",
    ) -> dict[str, Any]:
        """
        주문 정정.

        Args:
            order_no: 원주문번호.
            stock_code: 종목코드 6자리.
            new_price: 정정 가격.
            order_dvsn: 주문 구분 (``"00"``=지정가, ``"01"``=시장가).

        Returns:
            정정 결과 응답 딕셔너리.
        """
        tr_id = "TTTC0803U" if self._mode == "LIVE" else "VTTC0803U"

        body: dict[str, str] = {
            "CANO": self._account_no,
            "ACNT_PRDT_CD": self._account_product_code,
            "KRX_FWDG_ORD_ORGNO": "",
            "ORGN_ODNO": order_no,
            "ORD_DVSN": order_dvsn,
            "RVSE_CNCL_DVSN_CD": "01",  # 01=정정
            "ORD_QTY": "0",  # 전량 정정
            "ORD_UNPR": str(new_price),
            "QTY_ALL_ORD_YN": "Y",
        }

        logger.info(
            "kis_modify_order_request",
            order_no=order_no,
            stock_code=stock_code,
            new_price=new_price,
            order_dvsn=order_dvsn,
        )

        return await self.request(
            "POST",
            "/uapi/domestic-stock/v1/trading/order-rvsecncl",
            tr_id=tr_id,
            body=body,
        )

    # ------------------------------------------------------------------
    # 잔고/순위 조회 API
    # ------------------------------------------------------------------

    async def get_balance(self) -> dict[str, Any]:
        """
        주식 잔고 조회.

        Returns:
            잔고 응답 딕셔너리 (보유 종목 목록, 예수금 등 포함).
        """
        return await self.request(
            "GET",
            "/uapi/domestic-stock/v1/trading/inquire-balance",
            tr_id="TTTC8434R",
            params={
                "CANO": self._account_no,
                "ACNT_PRDT_CD": self._account_product_code,
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "02",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "01",
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            },
        )

    async def get_volume_rank(self) -> dict[str, Any]:
        """
        거래량 순위 조회.

        Returns:
            거래량 순위 응답 딕셔너리.
        """
        return await self.request(
            "GET",
            "/uapi/domestic-stock/v1/quotations/volume-rank",
            tr_id="FHPST01710000",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_COND_SCR_DIV_CODE": "20171",
                "FID_INPUT_ISCD": "0000",
                "FID_DIV_CLS_CODE": "0",
                "FID_BLNG_CLS_CODE": "0",
                "FID_TRGT_CLS_CODE": "111111111",
                "FID_TRGT_EXLS_CLS_CODE": "000000",
                "FID_INPUT_PRICE_1": "",
                "FID_INPUT_PRICE_2": "",
                "FID_VOL_CNT": "",
                "FID_INPUT_DATE_1": "",
            },
        )
