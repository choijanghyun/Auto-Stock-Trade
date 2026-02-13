"""
KIS API 호출 제한 관리 - Token Bucket 알고리즘

KIS REST API는 초당 20회 호출 제한이 있으며,
안전 마진을 두어 초당 18회로 제한한다.
Token Bucket 알고리즘으로 버스트 트래픽을 허용하면서도
지속적 초과 호출을 방지한다.
"""

from __future__ import annotations

import asyncio
import time

import structlog

logger = structlog.get_logger(__name__)


class RateLimiter:
    """
    Token Bucket 기반 API 호출 속도 제한기.

    동작 원리:
        - 버킷에 최대 ``max_tokens`` 개의 토큰이 저장된다.
        - 매초 ``refill_rate`` 개의 토큰이 자동 충전된다.
        - ``acquire()`` 호출 시 토큰 1개를 소비하며,
          토큰이 없으면 충전될 때까지 비동기 대기한다.

    Args:
        calls_per_second: 초당 허용 호출 수. 기본값 18 (KIS 20/sec에서 안전 마진).
        max_burst: 최대 버스트 허용량. 기본값은 ``calls_per_second``와 동일.
    """

    def __init__(
        self,
        calls_per_second: float = 18.0,
        max_burst: int | None = None,
    ) -> None:
        self._refill_rate: float = calls_per_second
        self._max_tokens: float = float(max_burst if max_burst is not None else int(calls_per_second))
        self._tokens: float = self._max_tokens
        self._last_refill: float = time.monotonic()
        self._lock: asyncio.Lock = asyncio.Lock()

        logger.info(
            "rate_limiter_initialized",
            refill_rate=self._refill_rate,
            max_tokens=self._max_tokens,
        )

    def _refill(self) -> None:
        """경과 시간에 비례하여 토큰을 충전한다."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        new_tokens = elapsed * self._refill_rate
        self._tokens = min(self._max_tokens, self._tokens + new_tokens)
        self._last_refill = now

    async def acquire(self) -> None:
        """
        토큰 1개를 획득한다.

        토큰이 부족하면 충전될 때까지 ``asyncio.sleep``으로 대기한다.
        asyncio.Lock을 사용해 동시 접근 시 순차 처리를 보장한다.
        """
        async with self._lock:
            self._refill()

            if self._tokens < 1.0:
                # 토큰 1개가 충전되기까지 필요한 대기 시간 계산
                deficit = 1.0 - self._tokens
                wait_seconds = deficit / self._refill_rate

                logger.debug(
                    "rate_limiter_waiting",
                    wait_seconds=round(wait_seconds, 4),
                    current_tokens=round(self._tokens, 2),
                )
                await asyncio.sleep(wait_seconds)

                # 대기 후 재충전
                self._refill()

            self._tokens -= 1.0

    @property
    def available_tokens(self) -> float:
        """현재 사용 가능한 토큰 수 (근사값, 잠금 없이 조회)."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        estimated = min(self._max_tokens, self._tokens + elapsed * self._refill_rate)
        return estimated

    def __repr__(self) -> str:
        return (
            f"RateLimiter(refill_rate={self._refill_rate}, "
            f"max_tokens={self._max_tokens}, "
            f"tokens={self._tokens:.2f})"
        )
