"""
KATS Inverse ETF Strategy (B2)

Bear-market strategy that takes long positions in inverse ETFs when
broad-market conditions are bearish.

Core logic:
    1. KOSPI is below both MA50 and MA200 (confirmed bear market).
    2. Trend confirmation: KOSPI MACD < 0 and declining.
    3. Buy inverse ETFs (e.g. KODEX 200 Inverse, KODEX Inverse 2X).
    4. Exit when KOSPI crosses back above MA50 or on target/stop.

Entry: KOSPI below MA50 + MA200 with trend confirmation.
Exit: KOSPI > MA50 or target +5 % or stop -3 %.
Focus: ETF grade, 17.5 % base position.

References:
    - Kathryn Staley, "The Art of Short Selling"
    - Martin Pring, "Technical Analysis Explained"
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import structlog

from kats.strategy.base_strategy import (
    BaseStrategy,
    StockCandidate,
    StrategyCategory,
    TradeSignal,
)

logger = structlog.get_logger(__name__)

# Known Korean inverse ETFs
INVERSE_ETF_CODES: Dict[str, str] = {
    "114800": "KODEX 인버스",
    "252670": "KODEX 200선물인버스2X",
    "145670": "KINDEX 인버스",
    "251340": "KODEX 코스닥150선물인버스",
    "261270": "KODEX 200선물인버스2X(합성)",
}


class InverseETFStrategy(BaseStrategy):
    """Inverse ETF strategy for bear markets.

    Only enters when broad-market indicators confirm a downtrend.
    Trades a pre-defined set of inverse ETFs listed on KRX.

    Category: BEAR (BEAR, STRONG_BEAR regimes).
    """

    def __init__(self) -> None:
        super().__init__("B2", StrategyCategory.BEAR)
        self.params: Dict[str, Any] = {
            "target_pct": 5.0,
            "stop_loss_pct": 3.0,
            "grade_target": ["ETF"],
            "position_pct": 17.5,
            "inverse_etf_codes": list(INVERSE_ETF_CODES.keys()),
        }

    # ── Market condition checks ───────────────────────────────────────────

    @staticmethod
    def _is_bear_market(kospi_indicators: Dict[str, Any]) -> bool:
        """Confirm bear market: KOSPI below MA50 and MA200."""
        price = kospi_indicators.get("kospi_close", 0)
        ma50 = kospi_indicators.get("kospi_ma50", 0)
        ma200 = kospi_indicators.get("kospi_ma200", 0)

        if price <= 0 or ma50 <= 0 or ma200 <= 0:
            return False

        return price < ma50 and price < ma200

    @staticmethod
    def _is_trend_declining(kospi_indicators: Dict[str, Any]) -> bool:
        """Check KOSPI MACD is negative and declining."""
        macd = kospi_indicators.get("kospi_macd", None)
        macd_signal = kospi_indicators.get("kospi_macd_signal", None)

        if macd is None or macd_signal is None:
            # Fallback: if price < MA50, assume declining
            price = kospi_indicators.get("kospi_close", 0)
            ma50 = kospi_indicators.get("kospi_ma50", 0)
            return price < ma50 if (price > 0 and ma50 > 0) else False

        return macd < 0 and macd < macd_signal

    @staticmethod
    def _should_exit_bear(kospi_indicators: Dict[str, Any]) -> bool:
        """Check if KOSPI has crossed back above MA50 (exit signal)."""
        price = kospi_indicators.get("kospi_close", 0)
        ma50 = kospi_indicators.get("kospi_ma50", 0)
        return price > ma50 if (price > 0 and ma50 > 0) else False

    # ── Scan ──────────────────────────────────────────────────────────────

    async def scan(
        self, candidates: List[StockCandidate]
    ) -> List[StockCandidate]:
        """Filter candidates to inverse ETFs only."""
        etf_codes = set(self.params["inverse_etf_codes"])
        filtered = [c for c in candidates if c.stock_code in etf_codes]
        self.log.info("scan_complete", strategy="B2", matched=len(filtered))
        return filtered

    # ── Signal generation ─────────────────────────────────────────────────

    async def generate_signal(
        self,
        stock: StockCandidate,
        market_data: Dict[str, Any],
    ) -> Optional[TradeSignal]:
        indicators: Dict[str, Any] = market_data.get("indicators", {})
        current_price: float = market_data["current_price"]

        # Must be an inverse ETF
        if stock.stock_code not in self.params["inverse_etf_codes"]:
            return None

        # Broad market must be bearish
        if not self._is_bear_market(indicators):
            return None
        if not self._is_trend_declining(indicators):
            return None

        # Check if we should be exiting instead
        if self._should_exit_bear(indicators):
            self.log.info(
                "bear_exit_condition",
                stock=stock.stock_code,
                note="KOSPI above MA50 -- skip new entry",
            )
            return None

        stop_loss = current_price * (1 - self.params["stop_loss_pct"] / 100)

        self.log.info(
            "signal_generated",
            stock=stock.stock_code,
            etf_name=INVERSE_ETF_CODES.get(stock.stock_code, "Unknown"),
        )

        return TradeSignal(
            stock_code=stock.stock_code,
            action="BUY",
            strategy_code="B2",
            entry_price=current_price,
            stop_loss=stop_loss,
            target_prices=[
                current_price * (1 + self.params["target_pct"] / 100),
            ],
            position_pct=self._adjust_position(stock.confidence, stock.grade),
            confidence=min(stock.confidence, 3),
            reason=(
                f"인버스 ETF 매수: "
                f"{INVERSE_ETF_CODES.get(stock.stock_code, stock.stock_code)}, "
                f"KOSPI 하락추세 확인 (MA50, MA200 하회, MACD 하락)"
            ),
            indicators_snapshot=self._capture_snapshot(indicators),
        )

    # ── Exit rules ────────────────────────────────────────────────────────

    def get_exit_rules(self) -> Dict[str, Any]:
        return {
            "stop_loss_pct": self.params["stop_loss_pct"],
            "target_prices_pct": [self.params["target_pct"]],
            "trailing_stop": False,
            "trailing_stop_pct": None,
            "time_exit": None,
            "max_holding_hours": None,
        }
