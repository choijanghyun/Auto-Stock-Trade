"""
KATS TradeJournal - Steenbarger + Van Tharp Integrated Auto-Journal

Records every trade with full market context, calculates R-multiples,
tracks emotional state, and flags rule violations.  Designed to build
the dataset that PerformanceAnalyzer and ReviewGenerator consume.

References:
- Brett Steenbarger, "Trading Psychology 2.0" -- emotional journaling
- Van K. Tharp, "Trade Your Way to Financial Freedom" -- R-multiple framework
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import structlog

from kats.database.models import Trade, TradeJournalEntry

logger = structlog.get_logger(__name__)


class TradeJournal:
    """
    Automatic trade journal that records every closed trade with its
    R-multiple, market snapshot, and optional emotional annotations.

    Parameters
    ----------
    repository
        Async repository (or session-factory wrapper) providing:
        - ``save_journal_entry(entry: TradeJournalEntry) -> TradeJournalEntry``
        - ``update_journal_entry(entry: TradeJournalEntry) -> TradeJournalEntry``
        - ``get_journal_entry_by_trade_id(trade_id: int) -> Optional[TradeJournalEntry]``
        - ``get_trades_between(start: datetime, end: datetime) -> List[Trade]``
        - ``get_journal_entries_between(start: datetime, end: datetime) -> List[TradeJournalEntry]``
    """

    def __init__(self, repository: Any) -> None:
        self._repo = repository

    # ------------------------------------------------------------------
    # Core recording
    # ------------------------------------------------------------------

    async def record_trade(
        self,
        trade: Trade,
        market_snapshot: Dict[str, Any],
    ) -> TradeJournalEntry:
        """
        Create a journal entry for a completed trade.

        Parameters
        ----------
        trade : Trade
            The ORM trade object.  Must have ``pnl_amount``, ``risk_amount``
            (1R), ``entry_price``, ``stop_loss_price``, and ``quantity`` set.
        market_snapshot : dict
            Point-in-time market indicators.  Expected keys (all optional --
            missing values are stored as ``None``):

            - ``ma5``, ``ma20``, ``ma50`` -- moving averages
            - ``rsi14`` -- 14-period RSI
            - ``vwap`` -- volume-weighted average price
            - ``bollinger_upper``, ``bollinger_lower``, ``bollinger_mid``
            - ``volume`` -- current volume
            - ``volume_ratio`` -- volume vs 20-day average
            - ``foreign_flow`` -- net foreign buy/sell amount
            - ``inst_flow`` -- net institutional buy/sell amount
            - ``kospi_close`` -- KOSPI index close
            - ``market_regime`` -- regime label (e.g. BULL / BEAR / NEUTRAL)

        Returns
        -------
        TradeJournalEntry
            The persisted journal entry.

        Notes
        -----
        If ``risk_amount`` (1R) is zero or ``None``, the R-multiple cannot
        be calculated and is stored as ``None`` with a warning logged.
        """
        # -- R-multiple (Van Tharp) ------------------------------------------
        r_multiple = self._calculate_r_multiple(trade)

        # -- Rule-violation check --------------------------------------------
        rule_violation: Optional[str] = None
        if r_multiple is not None and r_multiple < -1.0:
            rule_violation = (
                f"R-multiple {r_multiple:.2f} < -1.0: "
                "손절 규칙 위반 가능성 -- 1R 이상 손실 발생"
            )
            logger.warning(
                "rule_violation_detected",
                trade_id=trade.trade_id,
                r_multiple=round(r_multiple, 4),
                pnl=trade.pnl_amount,
                risk_1r=trade.risk_amount,
            )

        # -- Build snapshot JSON ---------------------------------------------
        snapshot = self._build_snapshot(market_snapshot)

        # -- Checklist score (from trade snapshot if available) ---------------
        checklist_score: Optional[float] = None
        if trade.snapshot_json and isinstance(trade.snapshot_json, dict):
            checklist_score = trade.snapshot_json.get("checklist_score")

        # -- Resolve stock grade at trade time --------------------------------
        stock_grade: Optional[str] = None
        if trade.stock:
            stock_grade = trade.stock.grade

        # -- Resolve strategy code --------------------------------------------
        entry_strategy: Optional[str] = None
        if trade.strategy:
            entry_strategy = trade.strategy.strategy_code

        # -- Persist ----------------------------------------------------------
        entry = TradeJournalEntry(
            trade_id=trade.trade_id,
            stock_grade=stock_grade,
            entry_strategy=entry_strategy,
            checklist_score=checklist_score,
            rule_violation=rule_violation,
            market_regime=snapshot.get("market_regime"),
        )
        # Attach full snapshot to the parent Trade object as well
        trade.snapshot_json = snapshot
        if r_multiple is not None:
            trade.r_multiple = r_multiple

        saved_entry = await self._repo.save_journal_entry(entry)

        logger.info(
            "trade_journaled",
            trade_id=trade.trade_id,
            stock_code=trade.stock_code,
            r_multiple=round(r_multiple, 4) if r_multiple is not None else None,
            stock_grade=stock_grade,
            strategy=entry_strategy,
            checklist_score=checklist_score,
            rule_violation=rule_violation is not None,
        )

        return saved_entry

    # ------------------------------------------------------------------
    # History retrieval
    # ------------------------------------------------------------------

    async def get_trade_history(
        self,
        start_date: date,
        end_date: date,
    ) -> List[TradeJournalEntry]:
        """
        Return all journal entries between *start_date* and *end_date* (inclusive).

        Parameters
        ----------
        start_date : date
            Start of the range (inclusive, 00:00:00 KST).
        end_date : date
            End of the range (inclusive, 23:59:59 KST).

        Returns
        -------
        list[TradeJournalEntry]
        """
        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end_date, datetime.max.time())
        entries = await self._repo.get_journal_entries_between(start_dt, end_dt)

        logger.debug(
            "trade_history_fetched",
            start=str(start_date),
            end=str(end_date),
            count=len(entries),
        )
        return entries

    # ------------------------------------------------------------------
    # Emotional annotation (Steenbarger)
    # ------------------------------------------------------------------

    async def add_emotional_note(
        self,
        trade_id: int,
        emotion_entry: str,
        emotion_during: str,
        lesson: str,
        improvement: str,
    ) -> TradeJournalEntry:
        """
        Attach an emotional self-assessment to an existing journal entry.

        This implements Steenbarger's journaling protocol: capture the
        emotion at entry, during the trade, what was learned, and what
        to improve next time.

        Parameters
        ----------
        trade_id : int
            The trade to annotate.
        emotion_entry : str
            Emotional state when the order was placed
            (e.g. "확신", "불안", "FOMO", "평온").
        emotion_during : str
            Emotional state while holding the position
            (e.g. "초조", "인내", "공포", "탐욕").
        lesson : str
            Key takeaway from this trade
            (e.g. "손절 지연으로 1R 초과 손실 발생").
        improvement : str
            Concrete next-step improvement
            (e.g. "손절가 도달 즉시 시장가 매도 자동화 확인").

        Returns
        -------
        TradeJournalEntry
            The updated journal entry.

        Raises
        ------
        ValueError
            If no journal entry exists for the given *trade_id*.
        """
        entry = await self._repo.get_journal_entry_by_trade_id(trade_id)
        if entry is None:
            raise ValueError(
                f"No journal entry found for trade_id={trade_id}. "
                "Record the trade first with record_trade()."
            )

        entry.emotion_entry = emotion_entry
        entry.emotion_during = emotion_during
        entry.lesson_learned = lesson
        entry.improvement = improvement

        updated = await self._repo.update_journal_entry(entry)

        logger.info(
            "emotional_note_added",
            trade_id=trade_id,
            emotion_entry=emotion_entry,
            emotion_during=emotion_during,
        )
        return updated

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _calculate_r_multiple(trade: Trade) -> Optional[float]:
        """
        Calculate the R-multiple for a trade.

        R-multiple = actual_pnl / risk_1r

        Where ``risk_1r`` is the initial risk amount (``trade.risk_amount``).
        If the risk amount equals ``|entry_price - stop_loss_price| * quantity``
        the R-multiple tells us how many multiples of the planned risk were
        actually captured (positive) or lost (negative).

        Returns ``None`` when insufficient data is available.
        """
        pnl = trade.pnl_amount
        risk_1r = trade.risk_amount

        if pnl is None:
            logger.warning(
                "r_multiple_skipped_no_pnl",
                trade_id=trade.trade_id,
            )
            return None

        if risk_1r is None or risk_1r == 0.0:
            # Attempt to derive 1R from entry/stop prices
            if (
                trade.entry_price is not None
                and trade.stop_loss_price is not None
                and trade.quantity is not None
                and trade.entry_price != trade.stop_loss_price
            ):
                risk_1r = abs(trade.entry_price - trade.stop_loss_price) * trade.quantity
            else:
                logger.warning(
                    "r_multiple_skipped_no_risk",
                    trade_id=trade.trade_id,
                    entry_price=trade.entry_price,
                    stop_loss_price=trade.stop_loss_price,
                )
                return None

        r_mult = pnl / risk_1r

        if r_mult < -1.0:
            logger.warning(
                "r_multiple_exceeded_1r_loss",
                trade_id=trade.trade_id,
                r_multiple=round(r_mult, 4),
                msg="손절 규칙 위반: 1R 이상 손실 발생",
            )

        return round(r_mult, 4)

    @staticmethod
    def _build_snapshot(market_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalise and sanitise the incoming market snapshot dict.

        Returns a clean dict with only the expected keys, casting numeric
        values to ``float`` where possible.
        """
        _NUMERIC_KEYS = (
            "ma5", "ma20", "ma50",
            "rsi14",
            "vwap",
            "bollinger_upper", "bollinger_lower", "bollinger_mid",
            "volume", "volume_ratio",
            "foreign_flow", "inst_flow",
            "kospi_close",
        )
        _STRING_KEYS = ("market_regime",)

        snapshot: Dict[str, Any] = {}

        for key in _NUMERIC_KEYS:
            raw = market_snapshot.get(key)
            if raw is not None:
                try:
                    snapshot[key] = float(raw)
                except (TypeError, ValueError):
                    snapshot[key] = None
                    logger.warning(
                        "snapshot_value_cast_failed",
                        key=key,
                        raw_value=raw,
                    )
            else:
                snapshot[key] = None

        for key in _STRING_KEYS:
            snapshot[key] = market_snapshot.get(key)

        return snapshot
