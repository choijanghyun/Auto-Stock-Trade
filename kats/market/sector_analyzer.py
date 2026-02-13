"""
KATS SectorAnalyzer - Sector Strength and Flow Analysis

Analyzes sector-level performance and institutional/foreign capital flow
to identify leading sectors for the strategy engine.

Korean market sectors follow the KRX (Korea Exchange) industry
classification.  Institutional and foreign investor flow data is
derived from daily buy/sell statistics per sector.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


# ── Data Structures ──────────────────────────────────────────────────────────


@dataclass
class SectorStrength:
    """Relative strength metrics for a single sector."""

    sector_name: str
    change_pct_1d: float = 0.0     # 1-day change (%)
    change_pct_5d: float = 0.0     # 5-day change (%)
    change_pct_20d: float = 0.0    # 20-day change (%)
    relative_strength: float = 0.0  # vs KOSPI benchmark
    rank: int = 0                   # 1 = strongest
    stock_count: int = 0            # number of constituent stocks
    advancing_count: int = 0        # stocks up today
    declining_count: int = 0        # stocks down today
    breadth_ratio: float = 0.0      # advancing / (advancing + declining)


@dataclass
class SectorFlow:
    """Institutional and foreign capital flow for a sector."""

    sector_name: str
    inst_net_buy: int = 0           # institutional net buy amount (KRW)
    foreign_net_buy: int = 0        # foreign net buy amount (KRW)
    inst_buy_streak: int = 0        # consecutive days of net buying
    foreign_buy_streak: int = 0     # consecutive days of net buying
    flow_direction: str = "NEUTRAL" # "BUY", "SELL", "NEUTRAL"
    updated_at: Optional[datetime] = None


# ── SectorAnalyzer ───────────────────────────────────────────────────────────


class SectorAnalyzer:
    """
    Sector-level market analysis engine.

    Responsibilities:
      1. Compute relative strength per sector vs KOSPI benchmark.
      2. Track institutional and foreign capital flow by sector.
      3. Identify the top-N leading sectors for strategy allocation.

    Parameters
    ----------
    rest_client : object, optional
        KISRestClient (or compatible) for fetching sector data via REST.
        If not provided, the analyzer operates in offline mode using
        data passed to ``analyze_sector_strength`` and ``update_flow``.
    benchmark_change : float
        KOSPI index daily change (%) used to compute relative strength.
        Updated via ``set_benchmark_change``.
    """

    # KRX standard sector names (representative, not exhaustive)
    KNOWN_SECTORS: List[str] = [
        "반도체",
        "자동차",
        "2차전지",
        "바이오",
        "금융",
        "철강/소재",
        "IT/소프트웨어",
        "건설",
        "유통",
        "화학",
        "에너지",
        "운송",
        "미디어/엔터",
        "음식료",
        "통신",
        "기계",
        "보험",
        "증권",
    ]

    def __init__(
        self,
        rest_client: Any = None,
        benchmark_change: float = 0.0,
    ) -> None:
        self.rest_client = rest_client
        self._benchmark_change: float = benchmark_change
        self._sector_strengths: Dict[str, SectorStrength] = {}
        self._sector_flows: Dict[str, SectorFlow] = {}

    # ── Benchmark ────────────────────────────────────────────────────────

    def set_benchmark_change(self, change_pct: float) -> None:
        """Update today's KOSPI index change for relative strength calc."""
        self._benchmark_change = change_pct

    # ── Sector Strength ──────────────────────────────────────────────────

    def analyze_sector_strength(
        self,
        sector_data: List[Dict[str, Any]],
    ) -> List[SectorStrength]:
        """
        Compute relative strength by sector.

        Parameters
        ----------
        sector_data : list[dict]
            Each dict represents one sector and must contain:
              - sector_name : str
              - change_pct_1d : float (today's change %)
              - change_pct_5d : float (5-day change %)
              - change_pct_20d : float (20-day change %)
              - stock_count : int (optional)
              - advancing_count : int (optional)
              - declining_count : int (optional)

        Returns
        -------
        list[SectorStrength]
            Sorted by relative strength (descending).
        """
        results: List[SectorStrength] = []

        for sd in sector_data:
            sector_name = sd.get("sector_name", "UNKNOWN")
            change_1d = float(sd.get("change_pct_1d", 0.0))
            change_5d = float(sd.get("change_pct_5d", 0.0))
            change_20d = float(sd.get("change_pct_20d", 0.0))
            stock_count = int(sd.get("stock_count", 0))
            advancing = int(sd.get("advancing_count", 0))
            declining = int(sd.get("declining_count", 0))

            # Relative strength = sector change - benchmark change
            # Weighted: 50% short-term (1d), 30% medium (5d), 20% longer (20d)
            rs = (
                (change_1d - self._benchmark_change) * 0.50
                + (change_5d - self._benchmark_change) * 0.30
                + (change_20d - self._benchmark_change) * 0.20
            )

            breadth = (
                advancing / (advancing + declining)
                if (advancing + declining) > 0
                else 0.0
            )

            strength = SectorStrength(
                sector_name=sector_name,
                change_pct_1d=change_1d,
                change_pct_5d=change_5d,
                change_pct_20d=change_20d,
                relative_strength=round(rs, 4),
                stock_count=stock_count,
                advancing_count=advancing,
                declining_count=declining,
                breadth_ratio=round(breadth, 4),
            )
            results.append(strength)

        # Sort by relative strength descending and assign ranks
        results.sort(key=lambda s: s.relative_strength, reverse=True)
        for i, s in enumerate(results):
            s.rank = i + 1

        # Cache for later queries
        self._sector_strengths = {s.sector_name: s for s in results}

        logger.info(
            "sector_strength_analyzed",
            sector_count=len(results),
            top_sector=results[0].sector_name if results else None,
            top_rs=results[0].relative_strength if results else None,
        )

        return results

    # ── Sector Flow ──────────────────────────────────────────────────────

    def update_flow(self, sector_name: str, flow_data: Dict[str, Any]) -> None:
        """
        Update institutional/foreign flow for a sector.

        Parameters
        ----------
        sector_name : str
        flow_data : dict
            - inst_net_buy : int (KRW)
            - foreign_net_buy : int (KRW)
            - inst_buy_streak : int (optional)
            - foreign_buy_streak : int (optional)
        """
        inst_net = int(flow_data.get("inst_net_buy", 0))
        foreign_net = int(flow_data.get("foreign_net_buy", 0))
        inst_streak = int(flow_data.get("inst_buy_streak", 0))
        foreign_streak = int(flow_data.get("foreign_buy_streak", 0))

        # Determine overall flow direction
        total_net = inst_net + foreign_net
        if total_net > 0 and (inst_net > 0 or foreign_net > 0):
            direction = "BUY"
        elif total_net < 0 and (inst_net < 0 or foreign_net < 0):
            direction = "SELL"
        else:
            direction = "NEUTRAL"

        sf = SectorFlow(
            sector_name=sector_name,
            inst_net_buy=inst_net,
            foreign_net_buy=foreign_net,
            inst_buy_streak=inst_streak,
            foreign_buy_streak=foreign_streak,
            flow_direction=direction,
            updated_at=datetime.now(),
        )
        self._sector_flows[sector_name] = sf

        logger.debug(
            "sector_flow_updated",
            sector=sector_name,
            direction=direction,
            inst_net=inst_net,
            foreign_net=foreign_net,
        )

    def get_sector_flow(self, sector: str) -> Optional[SectorFlow]:
        """
        Return institutional/foreign flow direction for a sector.

        Returns
        -------
        SectorFlow or None
            None if no flow data is available for the sector.
        """
        return self._sector_flows.get(sector)

    # ── Leading Sectors ──────────────────────────────────────────────────

    def get_leading_sectors(self, n: int = 3) -> List[SectorStrength]:
        """
        Return the top *n* sectors by relative strength.

        Sectors with both high relative strength AND positive capital
        flow are ranked higher.
        """
        if not self._sector_strengths:
            logger.warning("no_sector_strength_data", msg="Call analyze_sector_strength first")
            return []

        candidates = list(self._sector_strengths.values())

        # Sort by composite score: RS + flow bonus
        def composite_score(s: SectorStrength) -> float:
            base = s.relative_strength
            flow = self._sector_flows.get(s.sector_name)
            if flow is not None:
                if flow.flow_direction == "BUY":
                    base += 0.5  # bonus for capital inflow
                elif flow.flow_direction == "SELL":
                    base -= 0.3  # penalty for capital outflow
            # Breadth bonus: broad sector rally is more reliable
            base += s.breadth_ratio * 0.2
            return base

        candidates.sort(key=composite_score, reverse=True)
        top_n = candidates[:n]

        logger.info(
            "leading_sectors",
            sectors=[s.sector_name for s in top_n],
            scores=[round(composite_score(s), 4) for s in top_n],
        )

        return top_n

    # ── Query Helpers ────────────────────────────────────────────────────

    def get_sector_strength(self, sector_name: str) -> Optional[SectorStrength]:
        """Return strength data for a specific sector, or ``None``."""
        return self._sector_strengths.get(sector_name)

    def get_all_strengths(self) -> List[SectorStrength]:
        """Return all sector strengths sorted by rank."""
        return sorted(self._sector_strengths.values(), key=lambda s: s.rank)

    def is_sector_strong(self, sector_name: str, top_n: int = 5) -> bool:
        """Return ``True`` if *sector_name* is in the top *top_n* sectors."""
        s = self._sector_strengths.get(sector_name)
        if s is None:
            return False
        return s.rank <= top_n

    def get_sector_for_stock(
        self,
        stock_code: str,
        stock_sector_map: Dict[str, str],
    ) -> Optional[SectorStrength]:
        """
        Look up the sector strength for a stock given a stock->sector mapping.
        """
        sector_name = stock_sector_map.get(stock_code)
        if sector_name is None:
            return None
        return self._sector_strengths.get(sector_name)
