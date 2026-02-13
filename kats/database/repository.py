"""
KATS Repository - Async Data Access Layer

Provides a high-level async interface over the SQLAlchemy ORM models
for all database operations in the Korean stock auto-trading system.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any, Optional, Sequence

import structlog
from sqlalchemy import select, update, delete, func, and_
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from kats.database.models import (
    Base,
    DailyStat,
    DrawdownLog,
    EventCalendar,
    MonthlyStat,
    PaperAccount,
    Stock,
    Strategy,
    SystemConfig,
    Trade,
    TradeJournalEntry,
    create_all_tables,
)

logger = structlog.get_logger(__name__)


class Repository:
    """Async data access layer for KATS.

    Wraps an async SQLAlchemy engine and session factory to provide
    clean, typed CRUD helpers for every domain table.

    Usage::

        repo = Repository("postgresql+asyncpg://user:pw@localhost/kats")
        await repo.init_db()
        trade = Trade(stock_code="005930", ...)
        await repo.insert_trade(trade)
    """

    def __init__(
        self,
        db_url: str,
        echo: bool = False,
        pool_size: int = 5,
        max_overflow: int = 10,
    ) -> None:
        """Initialize the repository with an async database URL.

        Args:
            db_url: Async SQLAlchemy database URL.
                    e.g. ``postgresql+asyncpg://...`` or ``sqlite+aiosqlite:///...``
            echo: If True, log all SQL statements.
            pool_size: Connection pool size.
            max_overflow: Max overflow connections beyond pool_size.
        """
        self._engine: AsyncEngine = create_async_engine(
            db_url,
            echo=echo,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
        )
        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        logger.info("repository.initialized", db_url=db_url)

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    def session(self) -> AsyncSession:
        """Create a new async session. Use as async context manager."""
        return self._session_factory()

    # ------------------------------------------------------------------
    # Database initialisation
    # ------------------------------------------------------------------

    async def init_db(self) -> None:
        """Create all tables and seed initial system configuration.

        Safe to call multiple times; existing tables/rows are not overwritten.
        """
        await create_all_tables(self._engine)
        logger.info("repository.tables_created")

        await self._seed_system_config()
        await self._seed_default_strategies()
        logger.info("repository.seed_complete")

    async def _seed_system_config(self) -> None:
        """Insert default system configuration if not already present."""
        defaults: list[dict[str, Any]] = [
            {
                "config_key": "total_capital",
                "config_value": "10000000",
                "config_type": "float",
                "description": "총 투자 자본금 (원)",
            },
            {
                "config_key": "max_risk_per_trade_pct",
                "config_value": "1.0",
                "config_type": "float",
                "description": "거래당 최대 리스크 비율 (%)",
            },
            {
                "config_key": "max_position_count",
                "config_value": "10",
                "config_type": "int",
                "description": "최대 동시 보유 종목 수",
            },
            {
                "config_key": "trade_mode",
                "config_value": "PAPER",
                "config_type": "str",
                "description": "거래 모드 (LIVE/PAPER)",
            },
            {
                "config_key": "drawdown_yellow_pct",
                "config_value": "5.0",
                "config_type": "float",
                "description": "드로다운 YELLOW 경고 기준 (%)",
            },
            {
                "config_key": "drawdown_orange_pct",
                "config_value": "10.0",
                "config_type": "float",
                "description": "드로다운 ORANGE 경고 기준 (%)",
            },
            {
                "config_key": "drawdown_red_pct",
                "config_value": "15.0",
                "config_type": "float",
                "description": "드로다운 RED 경고 기준 (%)",
            },
            {
                "config_key": "drawdown_black_pct",
                "config_value": "20.0",
                "config_type": "float",
                "description": "드로다운 BLACK 매매중단 기준 (%)",
            },
            {
                "config_key": "market_regime",
                "config_value": "NEUTRAL",
                "config_type": "str",
                "description": "현재 시장 국면 (STRONG_BULL/BULL/NEUTRAL/BEAR/STRONG_BEAR)",
            },
        ]

        async with self._session_factory() as session:
            async with session.begin():
                for cfg in defaults:
                    existing = await session.get(SystemConfig, cfg["config_key"])
                    if existing is None:
                        session.add(SystemConfig(**cfg))
                        logger.debug(
                            "repository.seed_config",
                            key=cfg["config_key"],
                            value=cfg["config_value"],
                        )

    async def _seed_default_strategies(self) -> None:
        """Insert default strategy definitions if not already present."""
        defaults: list[dict[str, Any]] = [
            {
                "strategy_code": "BREAKOUT_PIVOT",
                "strategy_name": "피봇 돌파 전략",
                "category": "BULL",
                "description": "VCP/컵핸들 등 피봇 포인트 돌파 시 매수",
            },
            {
                "strategy_code": "PULLBACK_MA",
                "strategy_name": "이동평균 눌림목 전략",
                "category": "BULL",
                "description": "상승 추세 중 이동평균선 지지 확인 후 매수",
            },
            {
                "strategy_code": "GAP_FOLLOW",
                "strategy_name": "갭 추종 전략",
                "category": "BULL",
                "description": "실적 서프라이즈 등 갭 상승 후 추격 매수",
            },
            {
                "strategy_code": "MEAN_REVERSION",
                "strategy_name": "평균회귀 전략",
                "category": "NEUTRAL",
                "description": "과매도 구간에서 반등 매수",
            },
            {
                "strategy_code": "SHORT_HEDGE",
                "strategy_name": "숏 헤지 전략",
                "category": "BEAR",
                "description": "약세장 인버스 ETF 활용 헤지",
            },
        ]

        async with self._session_factory() as session:
            async with session.begin():
                for strat in defaults:
                    stmt = select(Strategy).where(
                        Strategy.strategy_code == strat["strategy_code"]
                    )
                    result = await session.execute(stmt)
                    if result.scalar_one_or_none() is None:
                        session.add(Strategy(**strat))
                        logger.debug(
                            "repository.seed_strategy",
                            code=strat["strategy_code"],
                        )

    # ------------------------------------------------------------------
    # Trade
    # ------------------------------------------------------------------

    async def insert_trade(self, trade: Trade) -> Trade:
        """Insert a new trade record.

        Args:
            trade: A Trade ORM instance (trade_id auto-generated).

        Returns:
            The same Trade instance with ``trade_id`` populated.
        """
        async with self._session_factory() as session:
            async with session.begin():
                session.add(trade)
            await session.refresh(trade)
            logger.info(
                "repository.trade_inserted",
                trade_id=trade.trade_id,
                stock_code=trade.stock_code,
                order_type=trade.order_type,
            )
            return trade

    async def get_trades_in_range(
        self,
        start: datetime,
        end: datetime,
        *,
        trade_mode: Optional[str] = None,
        stock_code: Optional[str] = None,
        strategy_id: Optional[int] = None,
        order_type: Optional[str] = None,
    ) -> Sequence[Trade]:
        """Retrieve trades within a datetime range.

        Args:
            start: Inclusive start datetime.
            end: Inclusive end datetime.
            trade_mode: Optional filter (LIVE/PAPER).
            stock_code: Optional stock code filter.
            strategy_id: Optional strategy ID filter.
            order_type: Optional order type filter (BUY/SELL).

        Returns:
            List of Trade records ordered by created_at ascending.
        """
        async with self._session_factory() as session:
            stmt = (
                select(Trade)
                .where(Trade.created_at >= start)
                .where(Trade.created_at <= end)
            )
            if trade_mode is not None:
                stmt = stmt.where(Trade.trade_mode == trade_mode)
            if stock_code is not None:
                stmt = stmt.where(Trade.stock_code == stock_code)
            if strategy_id is not None:
                stmt = stmt.where(Trade.strategy_id == strategy_id)
            if order_type is not None:
                stmt = stmt.where(Trade.order_type == order_type)

            stmt = stmt.order_by(Trade.created_at.asc())
            result = await session.execute(stmt)
            trades = result.scalars().all()
            logger.debug(
                "repository.trades_fetched",
                count=len(trades),
                start=str(start),
                end=str(end),
            )
            return trades

    # ------------------------------------------------------------------
    # Trade Journal
    # ------------------------------------------------------------------

    async def insert_journal(self, journal: TradeJournalEntry) -> TradeJournalEntry:
        """Insert a trade journal entry.

        Args:
            journal: A TradeJournalEntry ORM instance.

        Returns:
            The same instance with ``journal_id`` populated.
        """
        async with self._session_factory() as session:
            async with session.begin():
                session.add(journal)
            await session.refresh(journal)
            logger.info(
                "repository.journal_inserted",
                journal_id=journal.journal_id,
                trade_id=journal.trade_id,
            )
            return journal

    async def get_journal_by_trade_id(
        self, trade_id: int
    ) -> Optional[TradeJournalEntry]:
        """Retrieve a journal entry by its associated trade ID.

        Args:
            trade_id: The trade's primary key.

        Returns:
            TradeJournalEntry or None if not found.
        """
        async with self._session_factory() as session:
            stmt = select(TradeJournalEntry).where(
                TradeJournalEntry.trade_id == trade_id
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    # ------------------------------------------------------------------
    # DailyStat
    # ------------------------------------------------------------------

    async def insert_daily_stat(self, stat: DailyStat) -> DailyStat:
        """Insert or merge a daily statistics record.

        Args:
            stat: A DailyStat ORM instance.

        Returns:
            The merged DailyStat instance.
        """
        async with self._session_factory() as session:
            async with session.begin():
                merged = await session.merge(stat)
            logger.info(
                "repository.daily_stat_upserted", stat_date=str(stat.stat_date)
            )
            return merged

    async def get_daily_stat(self, stat_date: date) -> Optional[DailyStat]:
        """Retrieve daily statistics for a specific date.

        Args:
            stat_date: The date to look up.

        Returns:
            DailyStat or None.
        """
        async with self._session_factory() as session:
            return await session.get(DailyStat, stat_date)

    # ------------------------------------------------------------------
    # MonthlyStat
    # ------------------------------------------------------------------

    async def insert_monthly_stat(self, stat: MonthlyStat) -> MonthlyStat:
        """Insert or merge a monthly statistics record.

        Args:
            stat: A MonthlyStat ORM instance.

        Returns:
            The merged MonthlyStat instance.
        """
        async with self._session_factory() as session:
            async with session.begin():
                merged = await session.merge(stat)
            logger.info(
                "repository.monthly_stat_upserted", stat_month=stat.stat_month
            )
            return merged

    # ------------------------------------------------------------------
    # DrawdownLog
    # ------------------------------------------------------------------

    async def insert_drawdown_log(self, log: DrawdownLog) -> DrawdownLog:
        """Insert a drawdown event log.

        Args:
            log: A DrawdownLog ORM instance.

        Returns:
            The same instance with ``log_id`` populated.
        """
        async with self._session_factory() as session:
            async with session.begin():
                session.add(log)
            await session.refresh(log)
            logger.warning(
                "repository.drawdown_logged",
                log_id=log.log_id,
                level=log.level,
                drawdown_pct=log.drawdown_pct,
            )
            return log

    # ------------------------------------------------------------------
    # SystemConfig
    # ------------------------------------------------------------------

    async def get_system_config(
        self, config_key: str
    ) -> Optional[SystemConfig]:
        """Retrieve a system configuration value.

        Args:
            config_key: The configuration key.

        Returns:
            SystemConfig or None.
        """
        async with self._session_factory() as session:
            return await session.get(SystemConfig, config_key)

    async def set_system_config(
        self,
        config_key: str,
        config_value: str,
        config_type: Optional[str] = None,
        description: Optional[str] = None,
    ) -> SystemConfig:
        """Upsert a system configuration value.

        Args:
            config_key: The configuration key.
            config_value: The value (stored as string).
            config_type: Optional type hint (str/int/float/bool/json).
            description: Optional human-readable description.

        Returns:
            The upserted SystemConfig instance.
        """
        async with self._session_factory() as session:
            async with session.begin():
                existing = await session.get(SystemConfig, config_key)
                if existing is not None:
                    existing.config_value = config_value
                    if config_type is not None:
                        existing.config_type = config_type
                    if description is not None:
                        existing.description = description
                    cfg = existing
                else:
                    cfg = SystemConfig(
                        config_key=config_key,
                        config_value=config_value,
                        config_type=config_type,
                        description=description,
                    )
                    session.add(cfg)
            logger.info(
                "repository.config_set",
                key=config_key,
                value=config_value,
            )
            return cfg

    # ------------------------------------------------------------------
    # EventCalendar
    # ------------------------------------------------------------------

    async def insert_event(self, event: EventCalendar) -> EventCalendar:
        """Insert a calendar event.

        Args:
            event: An EventCalendar ORM instance.

        Returns:
            The same instance with ``event_id`` populated.
        """
        async with self._session_factory() as session:
            async with session.begin():
                session.add(event)
            await session.refresh(event)
            logger.info(
                "repository.event_inserted",
                event_id=event.event_id,
                event_date=str(event.event_date),
                event_name=event.event_name,
            )
            return event

    async def get_upcoming_events(
        self,
        from_date: Optional[date] = None,
        days_ahead: int = 7,
        *,
        event_type: Optional[str] = None,
        active_only: bool = True,
    ) -> Sequence[EventCalendar]:
        """Retrieve upcoming calendar events.

        Args:
            from_date: Start date (defaults to today).
            days_ahead: Number of days to look ahead.
            event_type: Optional filter by event type.
            active_only: If True, only return active events.

        Returns:
            List of EventCalendar records ordered by event_date ascending.
        """
        if from_date is None:
            from_date = date.today()
        end_date = from_date + timedelta(days=days_ahead)

        async with self._session_factory() as session:
            stmt = (
                select(EventCalendar)
                .where(EventCalendar.event_date >= from_date)
                .where(EventCalendar.event_date <= end_date)
            )
            if active_only:
                stmt = stmt.where(EventCalendar.is_active.is_(True))
            if event_type is not None:
                stmt = stmt.where(EventCalendar.event_type == event_type)

            stmt = stmt.order_by(EventCalendar.event_date.asc())
            result = await session.execute(stmt)
            events = result.scalars().all()
            logger.debug(
                "repository.upcoming_events",
                count=len(events),
                from_date=str(from_date),
                days_ahead=days_ahead,
            )
            return events

    # ------------------------------------------------------------------
    # PaperAccount
    # ------------------------------------------------------------------

    async def update_paper_account(
        self, account: PaperAccount
    ) -> PaperAccount:
        """Insert or update a paper trading account position.

        Args:
            account: A PaperAccount ORM instance.

        Returns:
            The merged PaperAccount instance.
        """
        async with self._session_factory() as session:
            async with session.begin():
                merged = await session.merge(account)
            logger.info(
                "repository.paper_account_upserted",
                account_id=merged.account_id,
                stock_code=merged.stock_code,
                quantity=merged.quantity,
            )
            return merged

    async def get_paper_account(
        self, stock_code: Optional[str] = None
    ) -> Sequence[PaperAccount]:
        """Retrieve paper account positions.

        Args:
            stock_code: Optional filter by stock code. If None, returns all.

        Returns:
            List of PaperAccount records.
        """
        async with self._session_factory() as session:
            stmt = select(PaperAccount)
            if stock_code is not None:
                stmt = stmt.where(PaperAccount.stock_code == stock_code)
            stmt = stmt.order_by(PaperAccount.stock_code.asc())
            result = await session.execute(stmt)
            return result.scalars().all()

    # ------------------------------------------------------------------
    # Strategy Stats
    # ------------------------------------------------------------------

    async def update_strategy_stats(self, strategy_id: int) -> Optional[Strategy]:
        """Recalculate and update aggregate statistics for a strategy.

        Counts total trades, wins, losses, and computes avg R-multiple
        and SQN score from all associated closed trades (those with an
        exit_price).

        Args:
            strategy_id: The strategy's primary key.

        Returns:
            Updated Strategy instance or None if the strategy does not exist.
        """
        async with self._session_factory() as session:
            async with session.begin():
                strategy = await session.get(Strategy, strategy_id)
                if strategy is None:
                    logger.warning(
                        "repository.strategy_not_found",
                        strategy_id=strategy_id,
                    )
                    return None

                # Aggregate closed trades for this strategy
                base_filter = and_(
                    Trade.strategy_id == strategy_id,
                    Trade.exit_price.isnot(None),
                )

                # Total / win / loss counts
                count_stmt = select(
                    func.count(Trade.trade_id).label("total"),
                    func.count(
                        func.nullif(Trade.pnl_amount > 0, False)
                    ).label("wins"),
                    func.count(
                        func.nullif(Trade.pnl_amount <= 0, False)
                    ).label("losses"),
                ).where(base_filter)
                counts = (await session.execute(count_stmt)).one()

                # Correct win/loss via explicit filters
                win_stmt = select(func.count(Trade.trade_id)).where(
                    base_filter, Trade.pnl_amount > 0
                )
                loss_stmt = select(func.count(Trade.trade_id)).where(
                    base_filter, Trade.pnl_amount <= 0
                )
                win_count = (await session.execute(win_stmt)).scalar() or 0
                loss_count = (await session.execute(loss_stmt)).scalar() or 0
                total_trades = win_count + loss_count

                # Average R-multiple
                r_stmt = select(
                    func.avg(Trade.r_multiple),
                    func.count(Trade.r_multiple),
                ).where(base_filter, Trade.r_multiple.isnot(None))
                r_row = (await session.execute(r_stmt)).one()
                avg_r: Optional[float] = float(r_row[0]) if r_row[0] is not None else None
                r_count: int = r_row[1] or 0

                # SQN = (avg_r / stddev_r) * sqrt(min(n, 100))
                sqn: Optional[float] = None
                if r_count >= 2:
                    stddev_stmt = select(
                        func.coalesce(
                            func.sqrt(
                                func.avg(Trade.r_multiple * Trade.r_multiple)
                                - func.avg(Trade.r_multiple) * func.avg(Trade.r_multiple)
                            ),
                            0,
                        )
                    ).where(base_filter, Trade.r_multiple.isnot(None))
                    stddev_r = (await session.execute(stddev_stmt)).scalar() or 0
                    if stddev_r > 0 and avg_r is not None:
                        import math
                        sqn = (avg_r / float(stddev_r)) * math.sqrt(min(r_count, 100))

                # Apply updates
                strategy.total_trades = total_trades
                strategy.win_count = win_count
                strategy.loss_count = loss_count
                strategy.avg_r_multiple = avg_r
                strategy.sqn_score = sqn

                logger.info(
                    "repository.strategy_stats_updated",
                    strategy_id=strategy_id,
                    total_trades=total_trades,
                    win_count=win_count,
                    loss_count=loss_count,
                    avg_r=avg_r,
                    sqn=sqn,
                )
                return strategy

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Dispose of the engine connection pool."""
        await self._engine.dispose()
        logger.info("repository.closed")
