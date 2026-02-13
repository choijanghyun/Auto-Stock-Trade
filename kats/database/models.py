"""
KATS Database Models - SQLAlchemy 2.0 ORM

Korean stock auto-trading system database schema.
All tables use async-compatible declarative mappings.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)


class Base(DeclarativeBase):
    """Base class for all ORM models."""
    pass


# ---------------------------------------------------------------------------
# Stock
# ---------------------------------------------------------------------------

class Stock(Base):
    """종목 마스터 - 종목 기본 정보 및 분석 지표"""
    __tablename__ = "stocks"

    stock_code: Mapped[str] = mapped_column(
        String(10), primary_key=True, comment="종목코드 (예: 005930)"
    )
    stock_name: Mapped[str] = mapped_column(
        String(100), nullable=False, comment="종목명"
    )
    market: Mapped[str] = mapped_column(
        String(10), nullable=False, comment="시장 (KOSPI/KOSDAQ)"
    )
    sector: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, comment="업종"
    )
    market_cap: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True, comment="시가총액 (원)"
    )

    # 종목 등급 & 추세 지표
    grade: Mapped[Optional[str]] = mapped_column(
        String(1), nullable=True, comment="종목 등급 (A/B/C/D)"
    )
    ma_50: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="50일 이동평균"
    )
    ma_150: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="150일 이동평균"
    )
    ma_200: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="200일 이동평균"
    )
    week52_high: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="52주 최고가"
    )
    week52_low: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="52주 최저가"
    )
    rs_rank: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="상대강도 순위 (0~100)"
    )

    # 거래량/회전율
    avg_volume_20d: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True, comment="20일 평균 거래량"
    )
    avg_turnover_20d: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="20일 평균 회전율 (%)"
    )

    # 펀더멘탈
    eps_growth_qoq: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="EPS 전분기 대비 성장률 (%)"
    )
    revenue_growth: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="매출 성장률 (%)"
    )
    op_margin_trend: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="영업이익률 추세"
    )

    # 수급
    inst_foreign_flow: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="기관/외국인 수급 점수"
    )

    # 종합 점수
    trend_template_score: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="추세 템플릿 점수"
    )
    canslim_score: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="CAN-SLIM 점수"
    )
    confidence_star: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=0, comment="확신도 (0~5)"
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, comment="활성 종목 여부"
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
        comment="최종 갱신 시각",
    )

    # Relationships
    trades: Mapped[list["Trade"]] = relationship(
        back_populates="stock", cascade="all, delete-orphan"
    )
    paper_accounts: Mapped[list["PaperAccount"]] = relationship(
        back_populates="stock", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_stocks_market_grade", "market", "grade"),
        Index("ix_stocks_rs_rank", "rs_rank"),
        Index("ix_stocks_is_active", "is_active"),
    )

    def __repr__(self) -> str:
        return f"<Stock {self.stock_code} {self.stock_name}>"


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class Strategy(Base):
    """전략 마스터 - 매매 전략 정의 및 통계"""
    __tablename__ = "strategies"

    strategy_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    strategy_code: Mapped[str] = mapped_column(
        String(50), unique=True, nullable=False, comment="전략 코드"
    )
    strategy_name: Mapped[str] = mapped_column(
        String(200), nullable=False, comment="전략 명칭"
    )
    category: Mapped[str] = mapped_column(
        String(10), nullable=False, comment="전략 카테고리 (BULL/BEAR/NEUTRAL)"
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, comment="전략 설명"
    )
    default_params: Mapped[Optional[dict]] = mapped_column(
        JSON, nullable=True, comment="기본 파라미터 (JSON)"
    )

    # 성과 통계
    total_trades: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="총 거래 수"
    )
    win_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="승리 수"
    )
    loss_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="패배 수"
    )
    avg_r_multiple: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="평균 R 배수"
    )
    sqn_score: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="SQN 점수"
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, comment="전략 활성 여부"
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
        comment="최종 갱신 시각",
    )

    # Relationships
    trades: Mapped[list["Trade"]] = relationship(
        back_populates="strategy", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_strategies_category", "category"),
        Index("ix_strategies_is_active", "is_active"),
    )

    def __repr__(self) -> str:
        return f"<Strategy {self.strategy_code}>"


# ---------------------------------------------------------------------------
# Trade
# ---------------------------------------------------------------------------

class Trade(Base):
    """거래 내역 - 매수/매도 실행 기록"""
    __tablename__ = "trades"

    trade_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    stock_code: Mapped[str] = mapped_column(
        String(10), ForeignKey("stocks.stock_code", ondelete="CASCADE"),
        nullable=False, comment="종목코드",
    )
    trade_mode: Mapped[str] = mapped_column(
        String(10), nullable=False, comment="거래 모드 (LIVE/PAPER)"
    )
    order_type: Mapped[str] = mapped_column(
        String(10), nullable=False, comment="주문 유형 (BUY/SELL)"
    )
    strategy_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("strategies.strategy_id", ondelete="SET NULL"),
        nullable=True, comment="전략 ID",
    )

    # 가격/수량
    entry_price: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="진입가"
    )
    exit_price: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="청산가"
    )
    quantity: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="수량"
    )
    amount: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="거래 금액"
    )

    # 손익
    pnl_amount: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="손익 금액 (원)"
    )
    pnl_percent: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="손익률 (%)"
    )
    r_multiple: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="R 배수 (리스크 대비 수익)"
    )

    # 리스크 관리
    stop_loss_price: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="손절가"
    )
    risk_amount: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="리스크 금액"
    )
    position_pct: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="포지션 비중 (%)"
    )

    # 피라미딩
    pyramid_stage: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=0, comment="피라미딩 단계"
    )
    parent_trade_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("trades.trade_id", ondelete="SET NULL"),
        nullable=True, comment="부모 거래 ID (피라미딩 원본)",
    )

    # 실행 품질
    slippage: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="슬리피지 (%)"
    )
    fill_time_ms: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, comment="체결 소요시간 (ms)"
    )

    # 스냅샷
    snapshot_json: Mapped[Optional[dict]] = mapped_column(
        JSON, nullable=True, comment="시점 스냅샷 (지표, 호가 등)"
    )

    # 시각
    entry_time: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True, comment="진입 시각"
    )
    exit_time: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True, comment="청산 시각"
    )
    holding_period_seconds: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, comment="보유 기간 (초)"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), comment="생성 시각"
    )

    # Relationships
    stock: Mapped["Stock"] = relationship(back_populates="trades")
    strategy: Mapped[Optional["Strategy"]] = relationship(back_populates="trades")
    journal_entry: Mapped[Optional["TradeJournalEntry"]] = relationship(
        back_populates="trade", uselist=False, cascade="all, delete-orphan"
    )
    children: Mapped[list["Trade"]] = relationship(
        back_populates="parent", remote_side=[trade_id],
        foreign_keys=[parent_trade_id],
    )
    parent: Mapped[Optional["Trade"]] = relationship(
        back_populates="children", remote_side=[trade_id],
        foreign_keys=[parent_trade_id],
    )

    __table_args__ = (
        Index("ix_trades_stock_code", "stock_code"),
        Index("ix_trades_strategy_id", "strategy_id"),
        Index("ix_trades_entry_time", "entry_time"),
        Index("ix_trades_created_at", "created_at"),
        Index("ix_trades_mode_type", "trade_mode", "order_type"),
    )

    def __repr__(self) -> str:
        return (
            f"<Trade {self.trade_id} {self.stock_code} "
            f"{self.order_type} qty={self.quantity}>"
        )


# ---------------------------------------------------------------------------
# TradeJournalEntry
# ---------------------------------------------------------------------------

class TradeJournalEntry(Base):
    """매매 일지 - 거래별 자기 평가 및 회고"""
    __tablename__ = "trade_journal_entries"

    journal_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    trade_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("trades.trade_id", ondelete="CASCADE"),
        unique=True, nullable=False, comment="거래 ID",
    )

    stock_grade: Mapped[Optional[str]] = mapped_column(
        String(1), nullable=True, comment="매매 시점 종목 등급"
    )
    entry_strategy: Mapped[Optional[str]] = mapped_column(
        String(200), nullable=True, comment="진입 전략"
    )
    checklist_score: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="체크리스트 점수"
    )

    # 심리 상태
    emotion_entry: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True, comment="진입 시 감정 상태"
    )
    emotion_during: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True, comment="보유 중 감정 상태"
    )

    # 규칙 준수
    rule_compliance: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="규칙 준수율 (%)"
    )
    rule_violation: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, comment="규칙 위반 내용"
    )

    # 복기
    lesson_learned: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, comment="교훈"
    )
    improvement: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, comment="개선 사항"
    )

    # 시장 맥락
    market_regime: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True, comment="시장 국면"
    )
    sector_flow: Mapped[Optional[str]] = mapped_column(
        String(200), nullable=True, comment="업종 수급"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), comment="생성 시각"
    )

    # Relationships
    trade: Mapped["Trade"] = relationship(back_populates="journal_entry")

    def __repr__(self) -> str:
        return f"<TradeJournalEntry {self.journal_id} trade={self.trade_id}>"


# ---------------------------------------------------------------------------
# DailyStat
# ---------------------------------------------------------------------------

class DailyStat(Base):
    """일별 통계 - 일일 성과 및 시장 요약"""
    __tablename__ = "daily_stats"

    stat_date: Mapped[date] = mapped_column(
        Date, primary_key=True, comment="통계 기준일"
    )

    # 거래 건수
    total_trades: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="총 거래 수"
    )
    buy_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="매수 건수"
    )
    sell_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="매도 건수"
    )
    win_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="승리 건수"
    )
    loss_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="패배 건수"
    )

    # 거래 금액
    total_buy_amount: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, default=0, comment="총 매수 금액"
    )
    total_sell_amount: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, default=0, comment="총 매도 금액"
    )

    # 일일 손익
    daily_pnl: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, default=0, comment="일일 손익 금액"
    )
    daily_pnl_pct: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, default=0, comment="일일 수익률 (%)"
    )
    cumulative_pnl: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, default=0, comment="누적 손익"
    )

    # 자본
    total_capital: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="총 자본"
    )
    cash_balance: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="현금 잔고"
    )
    cash_ratio: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="현금 비중 (%)"
    )

    # R 배수
    avg_r_multiple: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="평균 R 배수"
    )
    max_r_multiple: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="최대 R 배수"
    )
    min_r_multiple: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="최소 R 배수"
    )

    # 드로다운
    drawdown_pct: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, default=0, comment="당일 드로다운 (%)"
    )
    max_drawdown: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, default=0, comment="최대 드로다운 (%)"
    )

    # 시장
    market_regime: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True, comment="시장 국면"
    )
    kospi_close: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="KOSPI 종가"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), comment="생성 시각"
    )

    def __repr__(self) -> str:
        return f"<DailyStat {self.stat_date}>"


# ---------------------------------------------------------------------------
# MonthlyStat
# ---------------------------------------------------------------------------

class MonthlyStat(Base):
    """월별 통계 - 월간 성과 요약"""
    __tablename__ = "monthly_stats"

    stat_month: Mapped[str] = mapped_column(
        String(7), primary_key=True, comment="통계 기준월 (YYYY-MM)"
    )

    total_trades: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="총 거래 수"
    )
    win_rate: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="승률 (%)"
    )
    monthly_pnl: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, default=0, comment="월간 손익"
    )
    monthly_pnl_pct: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, default=0, comment="월간 수익률 (%)"
    )
    avg_r_multiple: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="평균 R 배수"
    )
    sqn_score: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="SQN 점수"
    )
    max_drawdown: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="최대 드로다운 (%)"
    )
    rule_compliance_rate: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="규칙 준수율 (%)"
    )

    # 전략/등급별 손익 JSON
    strategy_pnl_json: Mapped[Optional[dict]] = mapped_column(
        JSON, nullable=True, comment="전략별 손익 (JSON)"
    )
    grade_pnl_json: Mapped[Optional[dict]] = mapped_column(
        JSON, nullable=True, comment="등급별 손익 (JSON)"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), comment="생성 시각"
    )

    def __repr__(self) -> str:
        return f"<MonthlyStat {self.stat_month}>"


# ---------------------------------------------------------------------------
# DrawdownLog
# ---------------------------------------------------------------------------

class DrawdownLog(Base):
    """드로다운 로그 - 드로다운 이벤트 기록 및 대응"""
    __tablename__ = "drawdown_logs"

    log_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    level: Mapped[str] = mapped_column(
        String(10), nullable=False,
        comment="드로다운 레벨 (GREEN/YELLOW/ORANGE/RED/BLACK)",
    )
    drawdown_pct: Mapped[float] = mapped_column(
        Float, nullable=False, comment="드로다운 비율 (%)"
    )
    action_taken: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, comment="취한 조치"
    )
    resumed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True, comment="재개 시각"
    )
    recovery_trades: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, comment="회복까지 거래 수"
    )
    triggered_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(),
        comment="트리거 시각",
    )

    __table_args__ = (
        Index("ix_drawdown_logs_level", "level"),
        Index("ix_drawdown_logs_triggered_at", "triggered_at"),
    )

    def __repr__(self) -> str:
        return f"<DrawdownLog {self.log_id} {self.level} {self.drawdown_pct}%>"


# ---------------------------------------------------------------------------
# SystemConfig
# ---------------------------------------------------------------------------

class SystemConfig(Base):
    """시스템 설정 - 런타임 설정 키-값 저장소"""
    __tablename__ = "system_configs"

    config_key: Mapped[str] = mapped_column(
        String(100), primary_key=True, comment="설정 키"
    )
    config_value: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, comment="설정 값"
    )
    config_type: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True, comment="값 타입 (str/int/float/bool/json)"
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, comment="설정 설명"
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
        comment="최종 갱신 시각",
    )

    def __repr__(self) -> str:
        return f"<SystemConfig {self.config_key}={self.config_value}>"


# ---------------------------------------------------------------------------
# EventCalendar
# ---------------------------------------------------------------------------

class EventCalendar(Base):
    """이벤트 캘린더 - 시장 이벤트/휴장일/경제지표 일정"""
    __tablename__ = "event_calendars"

    event_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    event_date: Mapped[date] = mapped_column(
        Date, nullable=False, comment="이벤트 일자"
    )
    event_type: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="이벤트 유형"
    )
    event_name: Mapped[str] = mapped_column(
        String(200), nullable=False, comment="이벤트 명칭"
    )
    market_impact: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True, comment="시장 영향도 (HIGH/MEDIUM/LOW)"
    )
    trading_action: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True, comment="매매 조치 (HALT/REDUCE/NORMAL)"
    )
    cash_adjust_pct: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="현금 비중 조정 (%)"
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, comment="활성 여부"
    )

    __table_args__ = (
        Index("ix_event_calendars_date", "event_date"),
        Index("ix_event_calendars_type", "event_type"),
    )

    def __repr__(self) -> str:
        return f"<EventCalendar {self.event_date} {self.event_name}>"


# ---------------------------------------------------------------------------
# PaperAccount
# ---------------------------------------------------------------------------

class PaperAccount(Base):
    """모의투자 계좌 - 페이퍼 트레이딩 포지션 관리"""
    __tablename__ = "paper_accounts"

    account_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    stock_code: Mapped[str] = mapped_column(
        String(10), ForeignKey("stocks.stock_code", ondelete="CASCADE"),
        nullable=False, comment="종목코드",
    )
    quantity: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="보유 수량"
    )
    avg_price: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="평균 매수가"
    )
    current_price: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="현재가"
    )
    unrealized_pnl: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, default=0, comment="미실현 손익"
    )
    total_cash: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="총 현금"
    )
    total_equity: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, comment="총 평가금"
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
        comment="최종 갱신 시각",
    )

    # Relationships
    stock: Mapped["Stock"] = relationship(back_populates="paper_accounts")

    __table_args__ = (
        Index("ix_paper_accounts_stock_code", "stock_code"),
    )

    def __repr__(self) -> str:
        return f"<PaperAccount {self.account_id} {self.stock_code} qty={self.quantity}>"


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

async def create_all_tables(engine: AsyncEngine) -> None:
    """Create all tables defined in the ORM metadata.

    Uses ``run_sync`` to execute DDL statements against the async engine.
    Safe to call multiple times -- existing tables are not recreated.

    Args:
        engine: An async SQLAlchemy engine instance.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
