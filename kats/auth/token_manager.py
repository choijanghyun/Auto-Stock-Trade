"""
KATS Token Manager

Manages KIS API OAuth2 access tokens with:
- 24-hour token validity
- Automatic refresh 1 hour before expiry
- File-based token caching to avoid unnecessary re-issuance
- Async-compatible via aiohttp
"""

from __future__ import annotations

import json
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import aiohttp
import structlog

logger = structlog.get_logger(__name__)


class TokenManager:
    """
    KIS API access token automatic issuance and renewal manager.

    - Token validity: 24 hours
    - Auto-refresh: 1 hour before expiry
    - File caching: prevents redundant token issuance across restarts
    """

    TOKEN_ENDPOINT: str = "/oauth2/tokenP"
    REFRESH_MARGIN: timedelta = timedelta(hours=1)
    TOKEN_CACHE_FILE: str = "token_cache.json"

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        base_url: str,
        token_cache_dir: Optional[str] = None,
    ) -> None:
        self.app_key = app_key
        self.app_secret = app_secret
        self.base_url = base_url.rstrip("/")

        self._token: Optional[str] = None
        self._token_expired_at: Optional[datetime] = None
        self._lock = asyncio.Lock()

        cache_dir = Path(token_cache_dir) if token_cache_dir else Path.cwd()
        self._token_file = cache_dir / self.TOKEN_CACHE_FILE

        self._session: Optional[aiohttp.ClientSession] = None
        self._refresh_task: Optional[asyncio.Task[None]] = None

    # ── Public API ───────────────────────────────────────────────────

    async def get_token(self) -> str:
        """
        Return a valid access token. Refreshes automatically if expired
        or approaching expiry.
        """
        async with self._lock:
            if self._is_token_valid():
                return self._token  # type: ignore[return-value]

            # Attempt to load from file cache
            self._load_cached_token()
            if self._is_token_valid():
                logger.info(
                    "token_loaded_from_cache",
                    expired_at=str(self._token_expired_at),
                )
                self._schedule_refresh()
                return self._token  # type: ignore[return-value]

            # Issue a brand-new token
            token = await self._issue_new_token()
            self._schedule_refresh()
            return token

    async def close(self) -> None:
        """Clean up the HTTP session and cancel any scheduled refresh."""
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None

        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

        logger.info("token_manager_closed")

    @property
    def token(self) -> Optional[str]:
        """Current token value (may be None if not yet issued)."""
        return self._token

    @property
    def token_expired_at(self) -> Optional[datetime]:
        """Expiry datetime of the current token."""
        return self._token_expired_at

    # ── Token Issuance ───────────────────────────────────────────────

    async def _issue_new_token(self) -> str:
        """
        POST /oauth2/tokenP -- Issue a new access token.

        KIS API returns:
            {
                "access_token": "...",
                "access_token_token_expired": "2024-01-01 12:00:00",
                "token_type": "Bearer",
                "expires_in": 86400
            }
        """
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }

        url = f"{self.base_url}{self.TOKEN_ENDPOINT}"
        session = await self._get_session()

        logger.info("token_issuing", url=url)

        try:
            async with session.post(url, json=payload) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("token_issue_failed", error=str(exc))
            raise

        self._token = data["access_token"]
        self._token_expired_at = datetime.fromisoformat(
            data["access_token_token_expired"]
        )
        self._save_token_cache()

        logger.info(
            "token_issued",
            expired_at=str(self._token_expired_at),
        )
        return self._token

    # ── Validation ───────────────────────────────────────────────────

    def _is_token_valid(self) -> bool:
        """
        Check whether the current token is still valid.
        Considers the token invalid if it will expire within REFRESH_MARGIN.
        """
        if not self._token or not self._token_expired_at:
            return False
        return datetime.now() < (self._token_expired_at - self.REFRESH_MARGIN)

    # ── File Cache ───────────────────────────────────────────────────

    def _load_cached_token(self) -> None:
        """Load token and expiry from the JSON cache file."""
        if not self._token_file.exists():
            logger.debug("token_cache_not_found", path=str(self._token_file))
            return

        try:
            raw = self._token_file.read_text(encoding="utf-8")
            cache = json.loads(raw)
            self._token = cache["access_token"]
            self._token_expired_at = datetime.fromisoformat(cache["expired_at"])
            logger.debug(
                "token_cache_loaded",
                expired_at=str(self._token_expired_at),
            )
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            logger.warning("token_cache_corrupted", error=str(exc))
            self._token = None
            self._token_expired_at = None

    def _save_token_cache(self) -> None:
        """Persist the current token and expiry to a JSON cache file."""
        if not self._token or not self._token_expired_at:
            return

        cache = {
            "access_token": self._token,
            "expired_at": self._token_expired_at.isoformat(),
        }

        try:
            self._token_file.parent.mkdir(parents=True, exist_ok=True)
            self._token_file.write_text(
                json.dumps(cache, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.debug("token_cache_saved", path=str(self._token_file))
        except OSError as exc:
            logger.warning("token_cache_save_failed", error=str(exc))

    # ── Auto-Refresh Scheduling ──────────────────────────────────────

    def _schedule_refresh(self) -> None:
        """
        Schedule an automatic token refresh 1 hour before expiry.
        Uses asyncio.create_task with a sleep-based delay.
        """
        if not self._token_expired_at:
            return

        # Cancel any existing refresh task
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()

        refresh_at = self._token_expired_at - self.REFRESH_MARGIN
        delay = (refresh_at - datetime.now()).total_seconds()

        if delay <= 0:
            # Already past refresh time; refresh immediately on next get_token
            logger.debug("token_refresh_skipped_already_due")
            return

        self._refresh_task = asyncio.create_task(
            self._delayed_refresh(delay), name="token_auto_refresh"
        )
        logger.info(
            "token_refresh_scheduled",
            refresh_at=str(refresh_at),
            delay_seconds=round(delay, 1),
        )

    async def _delayed_refresh(self, delay: float) -> None:
        """Sleep for the given delay, then re-issue the token."""
        try:
            await asyncio.sleep(delay)
            async with self._lock:
                logger.info("token_auto_refreshing")
                await self._issue_new_token()
                self._schedule_refresh()
        except asyncio.CancelledError:
            logger.debug("token_refresh_task_cancelled")
        except Exception as exc:
            logger.error("token_auto_refresh_failed", error=str(exc))

    # ── HTTP Session ─────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazily create and return the shared aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Content-Type": "application/json; charset=utf-8"},
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session
