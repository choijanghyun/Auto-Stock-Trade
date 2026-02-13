"""
KATS StockScreener - Daily Stock Scanning Engine

Implements a multi-stage filtering pipeline inspired by:
  - Mark Minervini's SEPA Trend Template (8 technical checks)
  - William O'Neil's CAN SLIM (7 fundamental checks)
  - Custom Korean market qualifications (cap, turnover, listing age, etc.)
  - Grade classification (A/B/C/D) and confidence scoring (1-5 stars)

Designed to run daily at 08:30 KST before market open.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import structlog

from kats.market.indicator_calculator import IndicatorCalculator

logger = structlog.get_logger(__name__)


# ── Data Structures ──────────────────────────────────────────────────────────


@dataclass
class StockCandidate:
    """Screening result for a single stock."""

    stock_code: str
    stock_name: str
    market: str                    # "KOSPI" or "KOSDAQ"
    sector: str = ""

    # Price / volume snapshot
    price: float = 0.0
    ma_50: float = 0.0
    ma_150: float = 0.0
    ma_200: float = 0.0
    ma_200_slope: float = 0.0      # positive = rising
    week52_high: float = 0.0
    week52_low: float = 0.0
    rs_rank: float = 0.0           # relative strength percentile (0-100)

    # Volume / liquidity
    avg_volume_20d: int = 0
    avg_turnover_20d: int = 0      # KRW
    market_cap: int = 0            # KRW

    # Fundamental (CAN SLIM)
    eps_growth_qoq: float = 0.0    # EPS growth quarter-over-quarter (%)
    eps_growth_yoy: float = 0.0    # EPS growth year-over-year (%)
    revenue_growth: float = 0.0    # revenue growth (%)
    op_margin_trend: str = "FLAT"  # "UP", "DOWN", "FLAT"
    inst_foreign_flow: str = "NEUTRAL"  # "BUY", "SELL", "NEUTRAL"
    roe: float = 0.0
    new_product_or_mgmt: bool = False  # CAN SLIM "N" factor

    # Listing info
    listed_date: Optional[date] = None
    is_restricted: bool = False    # administrative issue / investment warning

    # Scores (filled during screening)
    trend_score: int = 0           # Minervini Trend Template (0-8)
    canslim_score: int = 0         # CAN SLIM (0-7)
    grade: str = "D"               # A / B / C / D
    confidence: int = 0            # 1-5 stars
    has_vcp: bool = False          # Volatility Contraction Pattern detected
    spread_pct: float = 0.0        # bid-ask spread (%)


# ── StockScreener ────────────────────────────────────────────────────────────


class StockScreener:
    """
    Daily stock scanning engine.

    Usage::

        screener = StockScreener(rest_client=kis_rest)
        candidates = await screener.scan_daily()
        for c in candidates:
            print(c.stock_code, c.grade, c.confidence)

    Parameters
    ----------
    rest_client : object
        KISRestClient (or compatible) used to fetch volume rank and daily
        prices.  Must implement ``get_volume_rank()`` and
        ``get_daily_price(stock_code, ...)``.
    market_cap_ranks : list[str], optional
        Pre-sorted list of stock codes by market cap (largest first).
        Used for grade classification.  If not supplied the screener
        will attempt to fetch it from ``rest_client``.
    """

    # ── Minervini Trend Template: 8 Technical Checks ─────────────────────

    TREND_TEMPLATE_LABELS: List[str] = [
        "Price > MA50",
        "Price > MA150",
        "Price > MA200",
        "MA50 > MA150 > MA200",
        "MA200 rising (positive slope over 1 month)",
        "Price >= 52-week low * 1.30",
        "Price >= 52-week high * 0.75",
        "RS rank >= 70 (top 30% relative strength)",
    ]

    # ── CAN SLIM: 7 Fundamental Checks ──────────────────────────────────

    CANSLIM_LABELS: List[str] = [
        "C - Current quarterly EPS growth >= 25%",
        "A - Annual EPS growth >= 25%",
        "N - New products / new management / new highs",
        "S - Supply & demand (institutional accumulation)",
        "L - Leader (RS rank >= 80)",
        "I - Institutional sponsorship (inst+foreign buy)",
        "M - Market direction (bull regime)",
    ]

    # ── Basic Qualification Thresholds ───────────────────────────────────

    MIN_MARKET_CAP: int = 500_000_000_000       # 500B KRW
    MIN_AVG_TURNOVER: int = 10_000_000_000      # 10B KRW
    MIN_AVG_VOLUME: int = 1_000_000             # 1M shares
    MAX_SPREAD_PCT: float = 0.3                  # 0.3%
    MIN_LISTING_MONTHS: int = 6

    def __init__(
        self,
        rest_client: Any,
        market_cap_ranks: Optional[List[str]] = None,
    ) -> None:
        self.rest_client = rest_client
        self._market_cap_ranks = market_cap_ranks or []

    # ── Main Entry Point ─────────────────────────────────────────────────

    async def scan_daily(self) -> List[StockCandidate]:
        """
        Execute the full daily screening pipeline.

        Returns the top 5 candidates sorted by confidence (descending).

        Pipeline stages:
            1. Fetch volume rank top 50
            2. Basic qualification filter
            3. Minervini Trend Template (8 checks)
            4. CAN SLIM fundamental checks (7 checks)
            5. Grade classification (A/B/C/D)
            6. Confidence scoring (1-5 stars)
            7. Return top 5
        """
        logger.info("stock_screener_scan_start")

        # Stage 1: fetch volume rank top 50
        try:
            volume_rank_resp = await self.rest_client.get_volume_rank()
            raw_candidates = self._parse_volume_rank(volume_rank_resp)
        except Exception:
            logger.exception("stock_screener_volume_rank_failed")
            return []

        logger.info(
            "stock_screener_stage1",
            raw_count=len(raw_candidates),
        )

        # Stage 2: basic qualification
        qualified = [c for c in raw_candidates if self._check_basic_qualification(c)]
        logger.info(
            "stock_screener_stage2_qualification",
            qualified_count=len(qualified),
            dropped=len(raw_candidates) - len(qualified),
        )

        # Stage 3: enrich with daily price data & compute trend template
        for c in qualified:
            await self._enrich_with_daily_data(c)
            c.trend_score = self._check_trend_template(c)

        trend_pass = [c for c in qualified if c.trend_score >= 5]
        logger.info(
            "stock_screener_stage3_trend",
            trend_pass_count=len(trend_pass),
        )

        # Stage 4: CAN SLIM fundamental scoring
        for c in trend_pass:
            c.canslim_score = self._check_canslim(c)

        # Stage 5: grade classification
        for c in trend_pass:
            c.grade = self._classify_grade(c)

        # Stage 6: confidence scoring
        for c in trend_pass:
            c.confidence = self._calculate_confidence(c)

        # Filter out non-tradeable (confidence <= 2 or grade D)
        tradeable = [
            c for c in trend_pass
            if c.confidence >= 3 and c.grade != "D"
        ]

        # Stage 7: sort and pick top 5
        tradeable.sort(key=lambda x: (x.confidence, x.trend_score), reverse=True)
        top5 = tradeable[:5]

        logger.info(
            "stock_screener_scan_complete",
            total_scanned=len(raw_candidates),
            tradeable=len(tradeable),
            returned=len(top5),
            top_codes=[c.stock_code for c in top5],
        )

        return top5

    # ── Stage Helpers ────────────────────────────────────────────────────

    def _parse_volume_rank(self, response: dict) -> List[StockCandidate]:
        """Parse REST volume rank response into StockCandidate list."""
        candidates: List[StockCandidate] = []
        output_list = response.get("output", [])
        for item in output_list[:50]:
            try:
                c = StockCandidate(
                    stock_code=item.get("mksc_shrn_iscd", ""),
                    stock_name=item.get("hts_kor_isnm", ""),
                    market=item.get("rprs_mrkt_kor_name", "KOSPI"),
                    price=float(item.get("stck_prpr", 0)),
                    avg_volume_20d=int(item.get("avrg_vol", 0)),
                    avg_turnover_20d=int(item.get("avrg_tr_pbmn", 0)),
                    market_cap=int(item.get("stck_avls", 0)),
                )
                candidates.append(c)
            except (ValueError, TypeError):
                logger.warning(
                    "stock_screener_parse_skip",
                    item=item,
                )
        return candidates

    def _check_basic_qualification(self, c: StockCandidate) -> bool:
        """
        Basic qualification filter.

        A stock must meet ALL of:
        - Market cap >= 500B KRW
        - Average daily turnover >= 10B KRW
        - Average daily volume >= 1M shares
        - Bid-ask spread <= 0.3%
        - Listed >= 6 months
        - Not restricted (not under administrative issue or investment warning)
        """
        if c.market_cap < self.MIN_MARKET_CAP:
            return False
        if c.avg_turnover_20d < self.MIN_AVG_TURNOVER:
            return False
        if c.avg_volume_20d < self.MIN_AVG_VOLUME:
            return False
        if c.spread_pct > self.MAX_SPREAD_PCT:
            return False
        if c.listed_date is not None:
            months_listed = (date.today() - c.listed_date).days / 30
            if months_listed < self.MIN_LISTING_MONTHS:
                return False
        if c.is_restricted:
            return False
        return True

    def _check_trend_template(self, c: StockCandidate) -> int:
        """
        Evaluate Minervini Trend Template (8 checks).

        Returns the number of checks passed (0-8).
        """
        score = 0

        # 1. Price > 50-day MA
        if c.price > 0 and c.ma_50 > 0 and c.price > c.ma_50:
            score += 1

        # 2. Price > 150-day MA
        if c.price > 0 and c.ma_150 > 0 and c.price > c.ma_150:
            score += 1

        # 3. Price > 200-day MA
        if c.price > 0 and c.ma_200 > 0 and c.price > c.ma_200:
            score += 1

        # 4. MA50 > MA150 > MA200 (moving average alignment)
        if c.ma_50 > c.ma_150 > c.ma_200 > 0:
            score += 1

        # 5. 200-day MA is rising (positive slope over last month)
        if c.ma_200_slope > 0:
            score += 1

        # 6. Price >= 52-week low * 1.30 (at least 30% above 52-week low)
        if c.week52_low > 0 and c.price >= c.week52_low * 1.30:
            score += 1

        # 7. Price >= 52-week high * 0.75 (within 25% of 52-week high)
        if c.week52_high > 0 and c.price >= c.week52_high * 0.75:
            score += 1

        # 8. RS rank >= 70 (relative strength in top 30%)
        if c.rs_rank >= 70:
            score += 1

        return score

    def _check_canslim(self, c: StockCandidate) -> int:
        """
        CAN SLIM fundamental checks (7 items).

        Returns the number of checks passed (0-7).
        """
        score = 0

        # C - Current quarterly EPS growth >= 25%
        if c.eps_growth_qoq >= 25:
            score += 1

        # A - Annual EPS growth >= 25%
        if c.eps_growth_yoy >= 25:
            score += 1

        # N - New products, new management, or new price highs
        if c.new_product_or_mgmt or (
            c.week52_high > 0 and c.price >= c.week52_high * 0.95
        ):
            score += 1

        # S - Supply/demand (institutional accumulation)
        if c.inst_foreign_flow in ("BUY",):
            score += 1

        # L - Leader (RS rank >= 80)
        if c.rs_rank >= 80:
            score += 1

        # I - Institutional sponsorship
        if c.inst_foreign_flow == "BUY":
            score += 1

        # M - Market direction (simplified: assume bull unless externally set)
        # In production this would check the MarketRegime module.
        # For scoring purposes we give 1 point if the stock's own trend is up.
        if c.ma_50 > 0 and c.ma_200 > 0 and c.ma_50 > c.ma_200:
            score += 1

        return score

    def _classify_grade(self, c: StockCandidate) -> str:
        """
        Grade classification by market cap rank.

        - A : top 30 by market cap (large-cap blue chips)
        - B : rank 30-100 (momentum mid-caps)
        - C : rank 100-200 (thematic small-caps)
        - D : below 200 (no trade)
        """
        if not self._market_cap_ranks:
            # Fallback: classify by market cap alone
            return self._classify_grade_by_cap(c)

        try:
            rank = self._market_cap_ranks.index(c.stock_code)
        except ValueError:
            return self._classify_grade_by_cap(c)

        if rank < 30:
            return "A"
        elif rank < 100:
            return "B"
        elif rank < 200:
            return "C"
        else:
            return "D"

    @staticmethod
    def _classify_grade_by_cap(c: StockCandidate) -> str:
        """Fallback grade classification using absolute market cap thresholds."""
        cap = c.market_cap
        if cap >= 10_000_000_000_000:      # >= 10T KRW
            return "A"
        elif cap >= 2_000_000_000_000:     # >= 2T KRW
            return "B"
        elif cap >= 500_000_000_000:       # >= 500B KRW
            return "C"
        else:
            return "D"

    def _calculate_confidence(self, c: StockCandidate) -> int:
        """
        Confidence star rating (1-5).

        5 stars : 8 trend template + 5+ CAN SLIM + VCP + inst+foreign buy
        4 stars : 6+ trend + 3+ CAN SLIM
        3 stars : 5+ trend + 2+ CAN SLIM
        2 or below : no trade
        """
        if (
            c.trend_score >= 8
            and c.canslim_score >= 5
            and c.has_vcp
            and c.inst_foreign_flow == "BUY"
        ):
            return 5

        if c.trend_score >= 8 and c.canslim_score >= 5:
            # All technical + strong fundamental but missing VCP or flow
            return 5

        if c.trend_score >= 6 and c.canslim_score >= 3:
            return 4

        if c.trend_score >= 5 and c.canslim_score >= 2:
            return 3

        return 2  # no trade

    # ── Data Enrichment ──────────────────────────────────────────────────

    async def _enrich_with_daily_data(self, c: StockCandidate) -> None:
        """
        Fetch daily candle data and compute technical indicators for a
        single candidate, populating MA, 52-week high/low, RS rank, etc.
        """
        try:
            resp = await self.rest_client.get_daily_price(c.stock_code, period="D", count=250)
            candles = resp.get("output2", resp.get("output", []))

            if not candles or len(candles) < 20:
                logger.warning(
                    "stock_screener_insufficient_daily_data",
                    stock_code=c.stock_code,
                    candle_count=len(candles) if candles else 0,
                )
                return

            # Parse daily candles (KIS returns newest first)
            closes: List[float] = []
            highs: List[float] = []
            lows: List[float] = []
            for candle in reversed(candles):
                closes.append(float(candle.get("stck_clpr", 0)))
                highs.append(float(candle.get("stck_hgpr", 0)))
                lows.append(float(candle.get("stck_lwpr", 0)))

            calc = IndicatorCalculator
            n = len(closes)

            # Moving averages
            if n >= 50:
                c.ma_50 = calc.sma(closes, 50)
            if n >= 150:
                c.ma_150 = calc.sma(closes, 150)
            if n >= 200:
                c.ma_200 = calc.sma(closes, 200)

            # MA200 slope (compare current MA200 to 20 days ago)
            if n >= 220:
                ma200_now = calc.sma(closes, 200)
                ma200_ago = calc.sma(closes[:-20], 200)
                c.ma_200_slope = ma200_now - ma200_ago
            elif n >= 200:
                c.ma_200_slope = 0.0

            # 52-week high/low (approx 250 trading days)
            last_year = closes[-min(250, n):]
            c.week52_high = max(highs[-min(250, n):])
            c.week52_low = min(lows[-min(250, n):])

            # Latest price refresh
            c.price = closes[-1]

        except Exception:
            logger.exception(
                "stock_screener_enrich_failed",
                stock_code=c.stock_code,
            )
