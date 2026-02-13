"""
KATS Market Regime Detector

Classifies the current KOSPI market regime into one of five states using
moving averages and advance/decline breadth data.  The detected regime
drives position-sizing, strategy selection, and cash-allocation decisions
across the entire trading system.

Regime Definitions
------------------
- STRONG_BULL : price > MA50 > MA200  AND  A/D ratio > 1.5
- BULL        : price > MA50  AND  price > MA200
- STRONG_BEAR : price < MA50  AND  price < MA200  AND  A/D ratio < 0.5
- BEAR        : price < MA200
- SIDEWAYS    : everything else

Usage:
    from kats.utils.market_regime import MarketRegimeDetector

    detector = MarketRegimeDetector()
    regime = detector.detect(kospi_data)
    cash_pct = detector.get_cash_allocation(regime)
    desc = detector.get_regime_description(regime)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

from kats.strategy.base_strategy import MarketRegime
from kats.utils.logger import get_logger

logger = get_logger(__name__)


# ============================================================================
# Cash Allocation Table
# ============================================================================

_CASH_ALLOCATION: Dict[MarketRegime, float] = {
    MarketRegime.STRONG_BULL: 10.0,   # Fully invested, minimal cash
    MarketRegime.BULL:        20.0,   # Moderate cash buffer
    MarketRegime.SIDEWAYS:    40.0,   # Defensive, larger cash cushion
    MarketRegime.BEAR:        60.0,   # Mostly cash, selective trades
    MarketRegime.STRONG_BEAR: 80.0,   # Capital preservation mode
}


# ============================================================================
# Regime Descriptions (Korean)
# ============================================================================

_REGIME_DESCRIPTION: Dict[MarketRegime, str] = {
    MarketRegime.STRONG_BULL: (
        "강한 상승장: 지수가 50일·200일 이평선 위에 있고 "
        "등락 비율이 1.5 이상입니다. "
        "공격적 매수 전략(S1~S5) 전면 가동, 현금 비중 최소화."
    ),
    MarketRegime.BULL: (
        "상승장: 지수가 주요 이평선 위에 위치합니다. "
        "추세 추종 전략 활성화, 적정 현금 비중 유지."
    ),
    MarketRegime.SIDEWAYS: (
        "횡보장: 명확한 방향성이 없습니다. "
        "레인지 전략(GR, B3) 위주로 운용하고 현금 비중을 높입니다."
    ),
    MarketRegime.BEAR: (
        "하락장: 지수가 200일 이평선 아래입니다. "
        "신규 매수를 자제하고 방어 전략(B1~B4)으로 전환합니다."
    ),
    MarketRegime.STRONG_BEAR: (
        "강한 하락장: 지수가 50일·200일 이평선 아래이고 "
        "등락 비율이 0.5 미만입니다. "
        "자본 보존 모드: 대부분 현금 보유, 최소한의 역발상 매매만 허용."
    ),
}


# ============================================================================
# MarketRegimeDetector
# ============================================================================

@dataclass
class KospiData:
    """Container for KOSPI market data required by the detector.

    Attributes:
        price: Current KOSPI index level (e.g. 2650.0).
        ma_50: 50-day simple moving average of KOSPI.
        ma_200: 200-day simple moving average of KOSPI.
        advance_decline_ratio: Ratio of advancing to declining issues
            (e.g. 1.2 means 120 advancing per 100 declining).
    """
    price: float
    ma_50: float
    ma_200: float
    advance_decline_ratio: float = 1.0


class MarketRegimeDetector:
    """Classifies the KOSPI market regime based on technical indicators.

    The detector is stateless -- each call to ``detect`` is independent.
    """

    # ── Detection ────────────────────────────────────────────────────────

    def detect(
        self,
        kospi_data: Union[KospiData, Dict[str, float], Any],
    ) -> MarketRegime:
        """Classify the current market regime.

        Decision tree:
          1. STRONG_BULL if price > MA50 > MA200 and A/D > 1.5
          2. BULL        if price > MA50 and price > MA200
          3. STRONG_BEAR if price < MA50 and price < MA200 and A/D < 0.5
          4. BEAR        if price < MA200
          5. SIDEWAYS    otherwise

        Args:
            kospi_data: A ``KospiData`` instance, a dict with keys
                ``price``, ``ma_50``, ``ma_200``, and optionally
                ``advance_decline_ratio``, or any object with those
                attributes.

        Returns:
            The detected ``MarketRegime`` enum member.
        """
        price, ma50, ma200, ad_ratio = self._extract_values(kospi_data)

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

        logger.info(
            "market_regime_detected",
            regime=regime.value,
            price=price,
            ma_50=ma50,
            ma_200=ma200,
            ad_ratio=ad_ratio,
        )

        return regime

    # ── Cash Allocation ──────────────────────────────────────────────────

    @staticmethod
    def get_cash_allocation(regime: MarketRegime) -> float:
        """Return the recommended cash percentage for the given regime.

        Args:
            regime: A ``MarketRegime`` enum member.

        Returns:
            Recommended cash allocation as a percentage (0-100).
        """
        return _CASH_ALLOCATION.get(regime, 40.0)

    # ── Description ──────────────────────────────────────────────────────

    @staticmethod
    def get_regime_description(regime: MarketRegime) -> str:
        """Return a Korean-language description of the given regime.

        Args:
            regime: A ``MarketRegime`` enum member.

        Returns:
            Human-readable description string.
        """
        return _REGIME_DESCRIPTION.get(regime, f"알 수 없는 시장 국면: {regime.value}")

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _extract_values(
        data: Union[KospiData, Dict[str, float], Any],
    ) -> tuple[float, float, float, float]:
        """Extract numeric values from various input types.

        Returns:
            ``(price, ma_50, ma_200, advance_decline_ratio)``
        """
        if isinstance(data, dict):
            price = float(data["price"])
            ma50 = float(data["ma_50"])
            ma200 = float(data["ma_200"])
            ad_ratio = float(data.get("advance_decline_ratio", 1.0))
        else:
            price = float(data.price)
            ma50 = float(data.ma_50)
            ma200 = float(data.ma_200)
            ad_ratio = float(getattr(data, "advance_decline_ratio", 1.0))

        return price, ma50, ma200, ad_ratio
