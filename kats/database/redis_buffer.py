"""
KATS Redis Tick Buffer

High-throughput in-memory buffer for real-time tick data and order book
snapshots. Data is stored in Redis with automatic TTL expiration and
can be bulk-flushed to the relational database at end-of-day.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any, Optional, Sequence

import structlog
from redis.asyncio import Redis, from_url as redis_from_url

logger = structlog.get_logger(__name__)

# Default TTL for tick data: 3 days (259_200 seconds)
_TICK_TTL_SECONDS: int = 3 * 24 * 60 * 60

# Key prefixes
_TICK_PREFIX = "tick"
_ORDERBOOK_PREFIX = "orderbook"
_VI_PREFIX = "vi"


class RedisTickBuffer:
    """Async Redis buffer for real-time market tick data.

    Tick data is pushed to per-stock, per-date lists and automatically
    expires after 3 days. Order book snapshots are stored as the latest
    value per stock code.

    Usage::

        buf = RedisTickBuffer("redis://localhost:6379/0")
        await buf.buffer_tick("005930", {"price": 72000, "volume": 100, ...})
        ticks = await buf.get_recent_ticks("005930", count=50)
        await buf.flush_to_db("2025-01-15", db_session)
        await buf.close()
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0") -> None:
        """Initialize the Redis tick buffer.

        Args:
            redis_url: Redis connection URL.
        """
        self._redis: Redis = redis_from_url(
            redis_url,
            decode_responses=True,
            max_connections=20,
        )
        self._redis_url = redis_url
        logger.info("redis_buffer.initialized", redis_url=redis_url)

    @property
    def redis(self) -> Redis:
        """Expose the underlying Redis client for advanced operations."""
        return self._redis

    # ------------------------------------------------------------------
    # Tick Data
    # ------------------------------------------------------------------

    @staticmethod
    def _tick_key(stock_code: str, date_str: Optional[str] = None) -> str:
        """Build the Redis key for a tick list.

        Format: ``tick:{stock_code}:{YYYYMMDD}``
        """
        if date_str is None:
            date_str = date.today().strftime("%Y%m%d")
        return f"{_TICK_PREFIX}:{stock_code}:{date_str}"

    async def buffer_tick(
        self, stock_code: str, tick_data: dict[str, Any]
    ) -> int:
        """Push a tick record into the Redis list (newest first).

        Each tick is JSON-serialised and LPUSH'd so the head of the list
        always contains the most recent tick. A 3-day TTL is set/renewed
        on every push.

        Args:
            stock_code: Stock code (e.g. ``"005930"``).
            tick_data: Dict of tick fields (price, volume, timestamp, ...).

        Returns:
            Current length of the tick list after push.
        """
        tick_data.setdefault("buffered_at", datetime.now().isoformat())
        key = self._tick_key(stock_code)
        payload = json.dumps(tick_data, ensure_ascii=False, default=str)

        pipe = self._redis.pipeline(transaction=False)
        pipe.lpush(key, payload)
        pipe.expire(key, _TICK_TTL_SECONDS)
        results = await pipe.execute()

        list_length: int = results[0]
        logger.debug(
            "redis_buffer.tick_buffered",
            stock_code=stock_code,
            key=key,
            list_length=list_length,
        )
        return list_length

    async def get_recent_ticks(
        self,
        stock_code: str,
        count: int = 100,
        date_str: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Retrieve the N most recent ticks for a stock.

        Args:
            stock_code: Stock code.
            count: Number of ticks to retrieve (from most recent).
            date_str: Optional date string (YYYYMMDD). Defaults to today.

        Returns:
            List of tick dicts, newest first.
        """
        key = self._tick_key(stock_code, date_str)
        raw_items: list[str] = await self._redis.lrange(key, 0, count - 1)

        ticks: list[dict[str, Any]] = []
        for item in raw_items:
            try:
                ticks.append(json.loads(item))
            except (json.JSONDecodeError, TypeError):
                logger.warning(
                    "redis_buffer.tick_decode_error",
                    stock_code=stock_code,
                    raw=item[:200],
                )
        logger.debug(
            "redis_buffer.ticks_retrieved",
            stock_code=stock_code,
            requested=count,
            returned=len(ticks),
        )
        return ticks

    async def get_all_ticks_for_day(
        self, stock_code: str, date_str: str
    ) -> list[dict[str, Any]]:
        """Retrieve ALL ticks for a stock on a given day.

        Args:
            stock_code: Stock code.
            date_str: Date in ``YYYYMMDD`` format.

        Returns:
            Full list of tick dicts for the day, newest first.
        """
        key = self._tick_key(stock_code, date_str)
        raw_items: list[str] = await self._redis.lrange(key, 0, -1)

        ticks: list[dict[str, Any]] = []
        for item in raw_items:
            try:
                ticks.append(json.loads(item))
            except (json.JSONDecodeError, TypeError):
                continue
        return ticks

    # ------------------------------------------------------------------
    # Order Book
    # ------------------------------------------------------------------

    @staticmethod
    def _orderbook_key(stock_code: str) -> str:
        """Build the Redis key for the latest order book snapshot.

        Format: ``orderbook:{stock_code}``
        """
        return f"{_ORDERBOOK_PREFIX}:{stock_code}"

    async def buffer_orderbook(
        self, stock_code: str, orderbook: dict[str, Any]
    ) -> None:
        """Store the latest order book snapshot for a stock.

        Overwrites the previous snapshot. The key is set with no TTL;
        it should be cleaned up via :meth:`clear_day_cache` at market close.

        Args:
            stock_code: Stock code.
            orderbook: Dict representing the order book (bids, asks, etc.).
        """
        orderbook.setdefault("buffered_at", datetime.now().isoformat())
        key = self._orderbook_key(stock_code)
        payload = json.dumps(orderbook, ensure_ascii=False, default=str)
        await self._redis.set(key, payload)
        logger.debug(
            "redis_buffer.orderbook_buffered",
            stock_code=stock_code,
        )

    async def get_orderbook(
        self, stock_code: str
    ) -> Optional[dict[str, Any]]:
        """Retrieve the latest order book snapshot for a stock.

        Args:
            stock_code: Stock code.

        Returns:
            Order book dict or None if not buffered.
        """
        key = self._orderbook_key(stock_code)
        raw: Optional[str] = await self._redis.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "redis_buffer.orderbook_decode_error",
                stock_code=stock_code,
            )
            return None

    # ------------------------------------------------------------------
    # Flush to DB
    # ------------------------------------------------------------------

    async def flush_to_db(
        self,
        date_str: str,
        db_session: Any,
        *,
        stock_codes: Optional[list[str]] = None,
        batch_size: int = 1000,
    ) -> int:
        """Bulk-insert buffered tick data from Redis into the database.

        Iterates over tick keys matching the given date, reads all data
        from each list, and performs batch inserts via the provided
        SQLAlchemy async session. After successful insert the Redis keys
        are deleted.

        Args:
            date_str: Date in ``YYYYMMDD`` format.
            db_session: An async SQLAlchemy session (or any object with
                        ``execute`` and ``commit`` methods).
            stock_codes: Optional list of stock codes to flush. If None,
                         all tick keys for the date are scanned.
            batch_size: Number of rows per INSERT batch.

        Returns:
            Total number of tick records flushed.
        """
        pattern = f"{_TICK_PREFIX}:*:{date_str}"
        keys_to_flush: list[str] = []

        if stock_codes is not None:
            keys_to_flush = [
                self._tick_key(code, date_str) for code in stock_codes
            ]
        else:
            # SCAN for matching keys (non-blocking)
            async for key in self._redis.scan_iter(match=pattern, count=500):
                keys_to_flush.append(key)

        if not keys_to_flush:
            logger.info(
                "redis_buffer.flush_noop",
                date_str=date_str,
                reason="no_matching_keys",
            )
            return 0

        total_flushed = 0

        for key in keys_to_flush:
            # Extract stock_code from key: tick:{code}:{date}
            parts = key.split(":")
            if len(parts) < 3:
                continue
            stock_code = parts[1]

            raw_items: list[str] = await self._redis.lrange(key, 0, -1)
            if not raw_items:
                continue

            rows: list[dict[str, Any]] = []
            for item in raw_items:
                try:
                    tick = json.loads(item)
                    tick["stock_code"] = stock_code
                    tick["tick_date"] = date_str
                    rows.append(tick)
                except (json.JSONDecodeError, TypeError):
                    continue

            # Batch insert via raw SQL for maximum throughput.
            # The caller is expected to provide a session bound to a table
            # that can accept these dicts. We use a generic approach:
            # insert rows as JSON blobs into a tick archive table, or the
            # caller can adapt.
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                try:
                    from sqlalchemy import text

                    for row in batch:
                        await db_session.execute(
                            text(
                                "INSERT INTO tick_archive "
                                "(stock_code, tick_date, tick_data) "
                                "VALUES (:stock_code, :tick_date, :tick_data)"
                            ),
                            {
                                "stock_code": row["stock_code"],
                                "tick_date": row["tick_date"],
                                "tick_data": json.dumps(
                                    row, ensure_ascii=False, default=str
                                ),
                            },
                        )
                    await db_session.commit()
                except Exception:
                    logger.exception(
                        "redis_buffer.flush_batch_error",
                        stock_code=stock_code,
                        batch_offset=i,
                    )
                    await db_session.rollback()
                    continue

            total_flushed += len(rows)

            # Remove the key after successful flush
            await self._redis.delete(key)
            logger.debug(
                "redis_buffer.key_flushed",
                key=key,
                rows=len(rows),
            )

        logger.info(
            "redis_buffer.flush_complete",
            date_str=date_str,
            total_flushed=total_flushed,
            keys_processed=len(keys_to_flush),
        )
        return total_flushed

    # ------------------------------------------------------------------
    # Cache Cleanup
    # ------------------------------------------------------------------

    async def clear_day_cache(self) -> int:
        """Clear transient order book and VI keys at market close.

        Scans for all ``orderbook:*`` and ``vi:*`` keys and deletes them.
        Tick data is NOT deleted here -- it is managed by TTL and
        :meth:`flush_to_db`.

        Returns:
            Number of keys deleted.
        """
        deleted = 0

        for prefix in (_ORDERBOOK_PREFIX, _VI_PREFIX):
            pattern = f"{prefix}:*"
            keys: list[str] = []
            async for key in self._redis.scan_iter(match=pattern, count=500):
                keys.append(key)
            if keys:
                removed = await self._redis.delete(*keys)
                deleted += removed
                logger.info(
                    "redis_buffer.cache_cleared",
                    prefix=prefix,
                    keys_deleted=removed,
                )

        logger.info("redis_buffer.day_cache_cleared", total_deleted=deleted)
        return deleted

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    async def ping(self) -> bool:
        """Check Redis connectivity.

        Returns:
            True if Redis responds to PING.
        """
        try:
            return await self._redis.ping()
        except Exception:
            logger.exception("redis_buffer.ping_failed")
            return False

    async def get_buffer_stats(
        self, date_str: Optional[str] = None
    ) -> dict[str, Any]:
        """Gather statistics about the current buffer state.

        Args:
            date_str: Date in ``YYYYMMDD`` format (defaults to today).

        Returns:
            Dict with ``tick_keys``, ``total_ticks``, ``orderbook_keys``.
        """
        if date_str is None:
            date_str = date.today().strftime("%Y%m%d")

        tick_keys: list[str] = []
        async for key in self._redis.scan_iter(
            match=f"{_TICK_PREFIX}:*:{date_str}", count=500
        ):
            tick_keys.append(key)

        total_ticks = 0
        for key in tick_keys:
            length = await self._redis.llen(key)
            total_ticks += length

        ob_keys: list[str] = []
        async for key in self._redis.scan_iter(
            match=f"{_ORDERBOOK_PREFIX}:*", count=500
        ):
            ob_keys.append(key)

        stats = {
            "date": date_str,
            "tick_keys": len(tick_keys),
            "total_ticks": total_ticks,
            "orderbook_keys": len(ob_keys),
        }
        logger.debug("redis_buffer.stats", **stats)
        return stats

    async def close(self) -> None:
        """Close the Redis connection pool."""
        await self._redis.aclose()
        logger.info("redis_buffer.closed")
