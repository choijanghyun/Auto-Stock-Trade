"""
KATS Strategy Selector

Determines the current market regime and returns the applicable set of
strategy instances for automated execution.

Market regime detection combines:
    * KOSPI price vs MA50/MA200 relationship (Elder Triple Screen Screen 1)
    * Advance/decline ratio (O'Neil Market Direction "M" factor)

Strategy mapping ensures that:
    * Bull strategies only run in uptrending markets.
    * Bear/defensive strategies only run in downtrending markets.
    * Neutral strategies (Grid, VB) operate across regimes.

References:
    - Alexander Elder, "Trading for a Living" (regime classification)
    - William O'Neil, "How to Make Money in Stocks" (M factor)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import structlog

from kats.strategy.base_strategy import BaseStrategy, MarketRegime
from kats.strategy.canslim_breakout import CANSLIMBreakoutStrategy
from kats.strategy.dead_cat_bounce import DeadCatBounceStrategy
from kats.strategy.dividend_switching import DividendSwitchingStrategy
from kats.strategy.gap_and_go import GapAndGoStrategy
from kats.strategy.grid_trading import GridTradingStrategy
from kats.strategy.inverse_etf import InverseETFStrategy
from kats.strategy.oversold_reversal import OversoldReversalStrategy
from kats.strategy.range_trading import RangeTradingStrategy
from kats.strategy.sepa_momentum import SEPAMomentumStrategy
from kats.strategy.triple_screen import TripleScreenStrategy
from kats.strategy.vwap_bounce import VWAPBounceStrategy
from kats.strategy.volatility_breakout import VolatilityBreakoutStrategy

logger = structlog.get_logger(__name__)


class StrategySelector:
    """Regime-aware strategy router.

    1. Detects the current market regime from KOSPI data.
    2. Returns only the strategies that are applicable for that regime.

    Usage::

        selector = StrategySelector()
        regime = selector.detect_regime(kospi_data)
        strategies = selector.select_strategies(regime)
        for strategy in strategies:
            candidates = await strategy.scan(all_candidates)
            ...
    """

    # ── Regime-to-strategy mapping ────────────────────────────────────────

    STRATEGY_MAP: Dict[MarketRegime, List[str]] = {
        MarketRegime.STRONG_BULL: ["S1", "S2", "S3", "S4", "S5", "VB", "GR"],
        MarketRegime.BULL:        ["S1", "S3", "S4", "S5", "VB", "GR"],
        MarketRegime.SIDEWAYS:    ["S5", "VB", "GR", "B3"],
        MarketRegime.BEAR:        ["B1", "B2", "B3", "B4", "GR"],
        MarketRegime.STRONG_BEAR: ["B1", "B2", "B4"],
    }

    def __init__(self) -> None:
        self.log = structlog.get_logger(__name__, component="StrategySelector")

        # Instantiate all strategies once
        self.all_strategies: List[BaseStrategy] = [
            SEPAMomentumStrategy(),        # S1
            GapAndGoStrategy(),            # S2
            CANSLIMBreakoutStrategy(),     # S3
            TripleScreenStrategy(),        # S4
            VWAPBounceStrategy(),          # S5
            VolatilityBreakoutStrategy(),  # VB
            GridTradingStrategy(),         # GR
            DividendSwitchingStrategy(),   # DS
            DeadCatBounceStrategy(),       # B1
            InverseETFStrategy(),          # B2
            RangeTradingStrategy(),        # B3
            OversoldReversalStrategy(),    # B4
        ]

        # Build lookup for O(1) access
        self._strategy_by_code: Dict[str, BaseStrategy] = {
            s.strategy_code: s for s in self.all_strategies
        }

        self.log.info(
            "initialised",
            total_strategies=len(self.all_strategies),
            codes=list(self._strategy_by_code.keys()),
        )

    # ── Regime detection ──────────────────────────────────────────────────

    def detect_regime(self, kospi_data: Dict[str, Any]) -> MarketRegime:
        """Classify the current market regime from KOSPI index data.

        Args:
            kospi_data: Dictionary with at least::

                {
                    "close": float,               # KOSPI closing price
                    "ma_50": float,                # 50-day MA
                    "ma_200": float,               # 200-day MA
                    "advance_decline_ratio": float, # > 1 means more advancers
                }

        Returns:
            The detected :class:`MarketRegime`.

        Detection rules (Elder + O'Neil):
            * STRONG_BULL: price > MA50 > MA200 AND A/D ratio > 1.5
            * BULL:        price > MA50 AND price > MA200
            * STRONG_BEAR: price < MA50 AND price < MA200 AND A/D ratio < 0.5
            * BEAR:        price < MA200
            * SIDEWAYS:    everything else
        """
        price = kospi_data.get("close", 0)
        ma50 = kospi_data.get("ma_50", 0)
        ma200 = kospi_data.get("ma_200", 0)
        ad_ratio = kospi_data.get("advance_decline_ratio", 1.0)

        if price <= 0 or ma50 <= 0 or ma200 <= 0:
            self.log.warning(
                "regime_detection_fallback",
                reason="incomplete KOSPI data",
                price=price,
                ma50=ma50,
                ma200=ma200,
            )
            return MarketRegime.SIDEWAYS

        regime: MarketRegime

        if price > ma50 > ma200 and ad_ratio > 1.5:
            regime = MarketRegime.STRONG_BULL
        elif price > ma50 and price > ma200:
            regime = MarketRegime.BULL
        elif price < ma50 and price < ma200 and ad_ratio < 0.5:
            regime = MarketRegime.STRONG_BEAR
        elif price < ma200:
            regime = MarketRegime.BEAR
        else:
            regime = MarketRegime.SIDEWAYS

        self.log.info(
            "regime_detected",
            regime=regime.value,
            price=price,
            ma50=ma50,
            ma200=ma200,
            ad_ratio=ad_ratio,
        )

        return regime

    # ── Strategy selection ────────────────────────────────────────────────

    def select_strategies(
        self, regime: MarketRegime
    ) -> List[BaseStrategy]:
        """Return active strategy instances applicable to *regime*.

        Strategies that have been deactivated (``is_active == False``) are
        excluded even if they appear in the regime mapping.
        """
        codes = self.STRATEGY_MAP.get(regime, [])
        selected = [
            s
            for s in self.all_strategies
            if s.strategy_code in codes and s.is_active
        ]

        self.log.info(
            "strategies_selected",
            regime=regime.value,
            requested_codes=codes,
            active_codes=[s.strategy_code for s in selected],
        )

        return selected

    # ── Utility methods ───────────────────────────────────────────────────

    def get_strategy(self, code: str) -> Optional[BaseStrategy]:
        """Look up a strategy instance by its code."""
        return self._strategy_by_code.get(code)

    def deactivate_strategy(self, code: str) -> bool:
        """Deactivate a strategy so it is excluded from selection.

        Returns ``True`` if the strategy was found and deactivated.
        """
        strategy = self._strategy_by_code.get(code)
        if strategy is None:
            self.log.warning("deactivate_not_found", code=code)
            return False
        strategy.is_active = False
        self.log.info("strategy_deactivated", code=code)
        return True

    def activate_strategy(self, code: str) -> bool:
        """Re-activate a previously deactivated strategy.

        Returns ``True`` if the strategy was found and activated.
        """
        strategy = self._strategy_by_code.get(code)
        if strategy is None:
            self.log.warning("activate_not_found", code=code)
            return False
        strategy.is_active = True
        self.log.info("strategy_activated", code=code)
        return True

    def list_strategies(self) -> List[Dict[str, Any]]:
        """Return a summary list of all registered strategies."""
        return [
            {
                "code": s.strategy_code,
                "class": s.__class__.__name__,
                "category": s.category.value,
                "active": s.is_active,
            }
            for s in self.all_strategies
        ]
