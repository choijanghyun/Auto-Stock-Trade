"""
KATS Hashkey Manager

Generates hashkeys for POST request body integrity verification via the
KIS API /uapi/hashkey endpoint. The hashkey is required for order-related
POST requests to prevent body tampering.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

import aiohttp
import structlog

logger = structlog.get_logger(__name__)


class HashkeyManager:
    """
    Request body integrity hashkey generator for KIS API POST requests.

    The KIS API requires a ``hashkey`` header on order-related POST requests.
    This manager calls ``POST /uapi/hashkey`` to obtain a hash of the request
    body, which is then included as the ``hashkey`` header in the actual
    order request.

    Usage:
        hm = HashkeyManager(app_key, app_secret, base_url)
        hashkey = await hm.get_hashkey({"CANO": "...", ...})
    """

    HASHKEY_ENDPOINT: str = "/uapi/hashkey"

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        base_url: str,
    ) -> None:
        self.app_key = app_key
        self.app_secret = app_secret
        self.base_url = base_url.rstrip("/")

        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()

    # ── Public API ───────────────────────────────────────────────────

    async def get_hashkey(self, body: Dict[str, Any]) -> str:
        """
        Generate a hashkey for the given POST request body.

        Args:
            body: The JSON body dict that will be sent in the actual
                  order request.

        Returns:
            The hashkey string to include as the ``hashkey`` header.

        Raises:
            aiohttp.ClientError: If the HTTP request fails.
            KeyError: If the response does not contain ``HASH``.
        """
        url = f"{self.base_url}{self.HASHKEY_ENDPOINT}"
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }

        session = await self._get_session()

        logger.debug("hashkey_requesting", body_keys=list(body.keys()))

        try:
            async with session.post(url, json=body, headers=headers) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            logger.error(
                "hashkey_request_failed",
                error=str(exc),
                url=url,
            )
            raise

        hashkey: str = data["HASH"]
        logger.debug("hashkey_generated", hashkey_prefix=hashkey[:8] + "...")
        return hashkey

    # ── Lifecycle ────────────────────────────────────────────────────

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        logger.info("hashkey_manager_closed")

    # ── HTTP Session ─────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazily create and return the shared aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    # ── Context Manager ──────────────────────────────────────────────

    async def __aenter__(self) -> HashkeyManager:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()
