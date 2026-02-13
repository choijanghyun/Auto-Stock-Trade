"""
KATS Constants Definitions

Market hours, event types, grade criteria, time zones, trading time windows,
and strategy code mappings for the Korean stock auto-trading system.

References:
- Design document Appendix A: Strategy code mapping
- Design document Appendix B: KOSPI optimal trading time windows
"""

from enum import Enum, unique
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Tuple
from zoneinfo import ZoneInfo


# ============================================================================
# Time Zones
# ============================================================================

KST = ZoneInfo("Asia/Seoul")
UTC = ZoneInfo("UTC")


# ============================================================================
# Market Hours (KST)
# ============================================================================

class MarketHours:
    """Korean stock market session boundaries (KST)."""

    # Pre-market
    SYSTEM_BOOT = "06:30"
    TOKEN_REFRESH = "08:00"
    PRE_MARKET_SCAN = "08:30"
    STRATEGY_PREPARE = "08:50"

    # Regular session
    MARKET_OPEN = "09:00"
    MARKET_CLOSE = "15:30"

    # Post-market
    POST_CANCEL_UNFILLED = "15:30"
    POST_REDIS_BULK_INSERT = "15:40"
    POST_DAILY_REPORT = "16:00"
    POST_CLEANUP = "16:30"

    # Broker maintenance window (no API calls)
    MAINTENANCE_START = "06:30"
    MAINTENANCE_END = "08:00"


# ============================================================================
# Trading Time Windows (Appendix B)
# ============================================================================

@dataclass(frozen=True)
class TradingWindow:
    """A time-based trading window with associated strategy codes."""
    start: str
    end: str
    label: str
    description: str
    active_strategies: Tuple[str, ...]
    allow_buy: bool
    allow_sell: bool


TRADING_WINDOWS: Tuple[TradingWindow, ...] = (
    TradingWindow(
        start="09:00",
        end="09:15",
        label="GAP_NOISE",
        description="Gap formation + max volatility noise -- observe only",
        active_strategies=(),
        allow_buy=False,
        allow_sell=True,
    ),
    TradingWindow(
        start="09:15",
        end="09:30",
        label="EARLY_DIRECTION",
        description="Early supply/demand direction confirmation",
        active_strategies=("S2",),
        allow_buy=True,
        allow_sell=True,
    ),
    TradingWindow(
        start="09:30",
        end="10:30",
        label="GOLDEN_HOUR",
        description="Highest win-rate period -- all strategies active",
        active_strategies=("S1", "S2", "S3", "S4", "S5", "VB"),
        allow_buy=True,
        allow_sell=True,
    ),
    TradingWindow(
        start="10:30",
        end="14:00",
        label="MIDDAY_LULL",
        description="Low volume, sideways movement -- range strategies only",
        active_strategies=("GR", "B3"),
        allow_buy=True,
        allow_sell=True,
    ),
    TradingWindow(
        start="14:00",
        end="15:20",
        label="CLOSING_RALLY",
        description="Pre-close volume increase -- liquidation focus",
        active_strategies=("S4", "B1", "B4"),
        allow_buy=False,
        allow_sell=True,
    ),
    TradingWindow(
        start="15:20",
        end="15:30",
        label="CLOSING_AUCTION",
        description="Closing single-price auction -- next-day gap plays",
        active_strategies=("DS",),
        allow_buy=True,
        allow_sell=True,
    ),
)


# ============================================================================
# Event Types
# ============================================================================

@unique
class EventType(str, Enum):
    """System-wide event types for the event bus."""

    # Market data events
    TICK = "TICK"
    ORDERBOOK = "ORDERBOOK"
    VI_TRIGGERED = "VI_TRIGGERED"
    VI_RELEASED = "VI_RELEASED"

    # Order lifecycle events
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_PARTIAL_FILLED = "ORDER_PARTIAL_FILLED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    ORDER_REJECTED = "ORDER_REJECTED"
    ORDER_AMENDED = "ORDER_AMENDED"
    ORDER_EXPIRED = "ORDER_EXPIRED"
    ORDER_ERROR = "ORDER_ERROR"

    # Signal events
    SIGNAL_BUY = "SIGNAL_BUY"
    SIGNAL_SELL = "SIGNAL_SELL"
    SIGNAL_STOP_LOSS = "SIGNAL_STOP_LOSS"
    SIGNAL_TRAILING_STOP = "SIGNAL_TRAILING_STOP"
    SIGNAL_TAKE_PROFIT = "SIGNAL_TAKE_PROFIT"

    # Risk events
    RISK_DAILY_LIMIT_HIT = "RISK_DAILY_LIMIT_HIT"
    RISK_MONTHLY_LIMIT_HIT = "RISK_MONTHLY_LIMIT_HIT"
    RISK_POSITION_LIMIT_HIT = "RISK_POSITION_LIMIT_HIT"
    RISK_SECTOR_LIMIT_HIT = "RISK_SECTOR_LIMIT_HIT"
    RISK_MARGIN_INSUFFICIENT = "RISK_MARGIN_INSUFFICIENT"

    # System events
    SYSTEM_START = "SYSTEM_START"
    SYSTEM_SHUTDOWN = "SYSTEM_SHUTDOWN"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    WEBSOCKET_CONNECTED = "WEBSOCKET_CONNECTED"
    WEBSOCKET_DISCONNECTED = "WEBSOCKET_DISCONNECTED"


# ============================================================================
# Order State
# ============================================================================

@unique
class OrderState(str, Enum):
    """Order lifecycle states."""

    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    PARTIAL_FILLED = "PARTIAL_FILLED"
    FILLED = "FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    AMEND_REQUESTED = "AMEND_REQUESTED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    ERROR = "ERROR"


# ============================================================================
# Strategy Category
# ============================================================================

@unique
class StrategyCategory(str, Enum):
    """Market regime categories for strategy selection."""

    BULL = "BULL"      # S1~S5
    BEAR = "BEAR"      # B1~B4
    NEUTRAL = "NEUTRAL"  # VB, GR, DS


# ============================================================================
# Stock Grade
# ============================================================================

@unique
class StockGrade(str, Enum):
    """
    Stock grade based on market capitalization ranking.

    A: Top 30 by market cap (ultra-large blue chips)
    B: Rank 30~100 (momentum mid-caps)
    C: Rank 100~200 (thematic small-caps)
    D: Trading prohibited
    """

    A = "A"
    B = "B"
    C = "C"
    D = "D"


# ============================================================================
# Grade Criteria
# ============================================================================

@dataclass(frozen=True)
class GradeCriteria:
    """Classification criteria for a stock grade."""
    market_cap_rank_start: int  # inclusive
    market_cap_rank_end: int    # inclusive
    max_single_position_pct: float
    max_concurrent_positions: Tuple[int, int]  # (min, max)
    max_grade_total_pct: float
    description: str


GRADE_CRITERIA: Dict[StockGrade, GradeCriteria] = {
    StockGrade.A: GradeCriteria(
        market_cap_rank_start=1,
        market_cap_rank_end=30,
        max_single_position_pct=30.0,
        max_concurrent_positions=(1, 2),
        max_grade_total_pct=50.0,
        description="Ultra-large blue chips",
    ),
    StockGrade.B: GradeCriteria(
        market_cap_rank_start=31,
        market_cap_rank_end=100,
        max_single_position_pct=20.0,
        max_concurrent_positions=(2, 3),
        max_grade_total_pct=40.0,
        description="Momentum mid-caps",
    ),
    StockGrade.C: GradeCriteria(
        market_cap_rank_start=101,
        market_cap_rank_end=200,
        max_single_position_pct=10.0,
        max_concurrent_positions=(1, 2),
        max_grade_total_pct=15.0,
        description="Thematic small-caps",
    ),
    StockGrade.D: GradeCriteria(
        market_cap_rank_start=201,
        market_cap_rank_end=999999,
        max_single_position_pct=0.0,
        max_concurrent_positions=(0, 0),
        max_grade_total_pct=0.0,
        description="Trading prohibited",
    ),
}


# ============================================================================
# Screening Thresholds
# ============================================================================

class ScreeningThresholds:
    """Minimum qualification criteria for stock screening."""

    MIN_MARKET_CAP_KRW: int = 500_000_000_000       # 5,000 억원
    MIN_AVG_TRADING_VALUE_20D: int = 10_000_000_000  # 100 억원 daily avg
    MIN_AVG_VOLUME_20D: int = 1_000_000              # 100 만주
    MAX_SPREAD_PCT: float = 0.3                      # 0.3% max bid-ask spread
    MIN_LISTING_MONTHS: int = 6
    MIN_TREND_TEMPLATE_SCORE: int = 5                # out of 8


# ============================================================================
# Strategy Code Mapping (Appendix A)
# ============================================================================

@dataclass(frozen=True)
class StrategySpec:
    """Specification for a single trading strategy."""
    code: str
    name: str
    name_kr: str
    category: StrategyCategory
    reference_books: Tuple[str, ...]
    target_grades: Tuple[StockGrade, ...]
    default_position_pct_range: Tuple[float, float]  # (min%, max%)


STRATEGY_SPECS: Dict[str, StrategySpec] = {
    "S1": StrategySpec(
        code="S1",
        name="SEPA Momentum Breakout",
        name_kr="SEPA 모멘텀 돌파",
        category=StrategyCategory.BULL,
        reference_books=("Minervini",),
        target_grades=(StockGrade.B,),
        default_position_pct_range=(15.0, 20.0),
    ),
    "S2": StrategySpec(
        code="S2",
        name="Gap & Go Pullback Buy",
        name_kr="Gap & Go 눌림목 매수",
        category=StrategyCategory.BULL,
        reference_books=("Cameron", "Aziz"),
        target_grades=(StockGrade.B, StockGrade.C),
        default_position_pct_range=(10.0, 15.0),
    ),
    "S3": StrategySpec(
        code="S3",
        name="CAN SLIM Breakout",
        name_kr="CAN SLIM 돌파 매매",
        category=StrategyCategory.BULL,
        reference_books=("O'Neil",),
        target_grades=(StockGrade.A,),
        default_position_pct_range=(20.0, 30.0),
    ),
    "S4": StrategySpec(
        code="S4",
        name="Triple Screen Trend Follow",
        name_kr="Triple Screen 추세 추종",
        category=StrategyCategory.BULL,
        reference_books=("Elder",),
        target_grades=(StockGrade.A, StockGrade.B),
        default_position_pct_range=(20.0, 25.0),
    ),
    "S5": StrategySpec(
        code="S5",
        name="VWAP Bounce",
        name_kr="VWAP 바운스",
        category=StrategyCategory.BULL,
        reference_books=("Carter", "Aziz"),
        target_grades=(StockGrade.A,),
        default_position_pct_range=(20.0, 30.0),
    ),
    "B1": StrategySpec(
        code="B1",
        name="Dead Cat Bounce",
        name_kr="데드캣 바운스",
        category=StrategyCategory.BEAR,
        reference_books=("Staley", "Turner"),
        target_grades=(StockGrade.A,),
        default_position_pct_range=(10.0, 15.0),
    ),
    "B2": StrategySpec(
        code="B2",
        name="Inverse ETF",
        name_kr="인버스 ETF",
        category=StrategyCategory.BEAR,
        reference_books=("Staley", "Pring"),
        target_grades=(),  # ETF-specific, no stock grade
        default_position_pct_range=(15.0, 20.0),
    ),
    "B3": StrategySpec(
        code="B3",
        name="Box Range Trading",
        name_kr="박스권 레인지 매매",
        category=StrategyCategory.BEAR,
        reference_books=("Nison", "Murphy"),
        target_grades=(StockGrade.A, StockGrade.B),
        default_position_pct_range=(10.0, 15.0),
    ),
    "B4": StrategySpec(
        code="B4",
        name="Oversold Contrarian",
        name_kr="과매도 역발상",
        category=StrategyCategory.BEAR,
        reference_books=("Steenbarger",),
        target_grades=(StockGrade.A,),
        default_position_pct_range=(10.0, 15.0),
    ),
    "VB": StrategySpec(
        code="VB",
        name="Volatility Breakout",
        name_kr="변동성 돌파",
        category=StrategyCategory.NEUTRAL,
        reference_books=("Larry Williams",),
        target_grades=(StockGrade.A, StockGrade.B),
        default_position_pct_range=(5.0, 25.0),  # variable
    ),
    "GR": StrategySpec(
        code="GR",
        name="Grid Trading",
        name_kr="그리드 매매",
        category=StrategyCategory.NEUTRAL,
        reference_books=(),
        target_grades=(StockGrade.A, StockGrade.B),
        default_position_pct_range=(5.0, 5.0),  # 5% per grid level
    ),
    "DS": StrategySpec(
        code="DS",
        name="Dividend Stock Switching",
        name_kr="배당주 스위칭",
        category=StrategyCategory.NEUTRAL,
        reference_books=(),
        target_grades=(StockGrade.A,),
        default_position_pct_range=(5.0, 25.0),  # variable
    ),
}

# Convenience sets for quick lookups
BULL_STRATEGY_CODES: FrozenSet[str] = frozenset(
    code for code, spec in STRATEGY_SPECS.items()
    if spec.category == StrategyCategory.BULL
)
BEAR_STRATEGY_CODES: FrozenSet[str] = frozenset(
    code for code, spec in STRATEGY_SPECS.items()
    if spec.category == StrategyCategory.BEAR
)
NEUTRAL_STRATEGY_CODES: FrozenSet[str] = frozenset(
    code for code, spec in STRATEGY_SPECS.items()
    if spec.category == StrategyCategory.NEUTRAL
)
ALL_STRATEGY_CODES: FrozenSet[str] = frozenset(STRATEGY_SPECS.keys())


# ============================================================================
# KIS WebSocket Subscription Keys
# ============================================================================

class WebSocketTrId:
    """KIS WebSocket transaction IDs for real-time data subscriptions."""

    REALTIME_PRICE = "H0STCNT0"     # Real-time execution price
    REALTIME_ORDERBOOK = "H0STASP0"  # Real-time orderbook (bid/ask)
    VI_STATUS = "H0STVI0"           # Volatility Interruption status
    ORDER_EXECUTION = "H0STCNC0"    # Order execution notification


# ============================================================================
# Daily Schedule
# ============================================================================

DAILY_SCHEDULE: Dict[str, str] = {
    "06:30": "System boot + health check + Redis connection verify",
    "08:00": "Token refresh check",
    "08:30": "Stock screening (StockScreener.scan_daily) via REST",
    "08:50": "Strategy selection + trading scenario preparation",
    "09:00": "WebSocket subscribe + VI monitor start + OrderTracker start",
    "09:00~15:30": "Real-time trading loop (quotes=cache, orders=REST)",
    "15:30": "Market close -- OrderTracker cancel all unfilled orders",
    "15:40": "Redis -> DB Bulk Insert + daily statistics aggregation",
    "16:00": "Performance report generation + notification dispatch",
    "16:30": "WebSocket disconnect + Redis cache reset",
}


# ============================================================================
# Order Defaults
# ============================================================================

class OrderDefaults:
    """Default values for order management."""

    DEFAULT_ORDER_TTL_SEC: int = 300   # 5 minutes
    AMEND_THRESHOLD_PCT: float = 80.0  # Attempt amend at 80% of TTL elapsed
    MAX_PARTIAL_FILL_WAIT_SEC: int = 60


# ============================================================================
# Confidence Levels
# ============================================================================

class ConfidenceLevel:
    """Confidence score boundaries (1-5 stars)."""

    MIN_SCORE: int = 1
    MAX_SCORE: int = 5
    AGGRESSIVE_ENTRY_THRESHOLD: int = 4  # 4+ stars => max grade allocation
    DEFAULT_ENTRY_THRESHOLD: int = 3     # 3 stars => base allocation
    CONSERVATIVE_THRESHOLD: int = 2      # 2 stars => 50% allocation reduction
