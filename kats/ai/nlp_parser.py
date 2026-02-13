"""
KATS Natural Language Parser

Parses Korean natural-language trading commands into structured intents and
entities.  Designed for the MCP (Model Context Protocol) command pipeline so
that users can issue orders, queries, and strategy changes in plain Korean
through Telegram or Slack.

Supported intents
-----------------
- conditional_buy     : "삼성전자 5% 이상 오르면 100주 매수"
- conditional_sell    : "카카오 3% 하락하면 전량 매도"
- modify_stop_loss    : "SK하이닉스 손절가 10만원으로 변경"
- performance_report  : "오늘 성과 보여줘", "이번 달 수익률"
- status_inquiry      : "현재 포지션 알려줘", "잔고 조회"
- strategy_setup      : "삼전에 VB 전략 적용해줘"
- cancel_order        : "삼성전자 주문 취소해줘"
- unknown             : fallback

Usage:
    from kats.ai.nlp_parser import NLPParser

    parser = NLPParser()
    intent = parser.parse_intent("삼성전자 5% 오르면 100주 매수해줘")
    entities = parser.extract_entities("삼성전자 5% 오르면 100주 매수해줘")
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from kats.utils.logger import get_logger

logger = get_logger(__name__)


# ============================================================================
# Stock Aliases  (Korean name / abbreviation -> 6-digit stock code)
# ============================================================================

STOCK_ALIASES: Dict[str, str] = {
    # ── Full names ────────────────────────────────────────────────────────
    "삼성전자":       "005930",
    "SK하이닉스":     "000660",
    "현대차":         "005380",
    "현대자동차":     "005380",
    "네이버":         "035420",
    "NAVER":          "035420",
    "카카오":         "035720",
    "LG에너지솔루션": "373220",
    "LG에너지":       "373220",
    "삼성SDI":        "006400",
    "기아":           "000270",
    "기아차":         "000270",
    "POSCO홀딩스":    "005490",
    "포스코홀딩스":   "005490",
    "포스코":         "005490",
    "셀트리온":       "068270",
    "현대모비스":     "012330",
    "KB금융":         "105560",
    "신한지주":       "055550",
    "삼성바이오로직스": "207940",
    "삼성물산":       "028260",
    "LG화학":         "051910",
    "SK이노베이션":   "096770",
    "한국전력":       "015760",
    "한전":           "015760",
    "카카오뱅크":     "323410",
    "크래프톤":       "259960",

    # ── Common abbreviations ─────────────────────────────────────────────
    "삼전":           "005930",
    "하닉":           "000660",
    "하이닉스":       "000660",
    "현차":           "005380",
    "네바":           "035420",
    "카카":           "035720",
    "셀트":           "068270",
    "삼바":           "207940",
    "엘화":           "051910",
    "삼디":           "006400",
    "포홀":           "005490",
}

# Reverse mapping: code -> canonical Korean name (first full-name entry wins)
_CODE_TO_NAME: Dict[str, str] = {}
for _name, _code in STOCK_ALIASES.items():
    if _code not in _CODE_TO_NAME and len(_name) >= 2:
        _CODE_TO_NAME[_code] = _name


# ============================================================================
# Condition Patterns  (regex-based entity extraction)
# ============================================================================

@dataclass(frozen=True)
class _ConditionPattern:
    """A compiled regex pattern with its semantic label."""
    label: str
    pattern: re.Pattern[str]


CONDITION_PATTERNS: List[_ConditionPattern] = [
    # ── Price up by percentage ───────────────────────────────────────────
    _ConditionPattern(
        label="price_up_pct",
        pattern=re.compile(
            r"(?P<threshold>\d+(?:\.\d+)?)\s*%\s*(?:이상\s*)?(?:오르|상승|올라)",
        ),
    ),
    # ── Price down by percentage ─────────────────────────────────────────
    _ConditionPattern(
        label="price_down_pct",
        pattern=re.compile(
            r"(?P<threshold>\d+(?:\.\d+)?)\s*%\s*(?:이상\s*)?(?:내리|하락|떨어|빠지)",
        ),
    ),
    # ── Price above absolute value ───────────────────────────────────────
    _ConditionPattern(
        label="price_above",
        pattern=re.compile(
            r"(?P<price>\d[\d,]*)\s*원?\s*(?:이상|넘으면|돌파|위)",
        ),
    ),
    # ── Price below absolute value ───────────────────────────────────────
    _ConditionPattern(
        label="price_below",
        pattern=re.compile(
            r"(?P<price>\d[\d,]*)\s*원?\s*(?:이하|밑으로|아래|미만)",
        ),
    ),
]

# ── Quantity pattern ─────────────────────────────────────────────────────
_QTY_PATTERN = re.compile(r"(?P<qty>\d[\d,]*)\s*주")
_QTY_ALL_PATTERN = re.compile(r"전량|전부|모두|다\s*(?:매도|팔아)")

# ── Price pattern (standalone, for stop-loss changes etc.) ───────────────
_PRICE_PATTERN = re.compile(r"(?P<price>\d[\d,]*)\s*원")

# ── Stock code pattern (direct 6-digit code input) ──────────────────────
_CODE_PATTERN = re.compile(r"\b(?P<code>\d{6})\b")


# ============================================================================
# Intent Classification Keywords
# ============================================================================

_INTENT_KEYWORDS: Dict[str, List[str]] = {
    "conditional_buy": [
        "매수", "사줘", "사자", "매입", "사", "들어가", "진입",
    ],
    "conditional_sell": [
        "매도", "팔아", "팔자", "청산", "처분", "빠져",
    ],
    "modify_stop_loss": [
        "손절", "스탑로스", "stop.?loss", "손절가", "손절선",
        "스탑", "SL",
    ],
    "performance_report": [
        "성과", "수익률", "실적", "리포트", "보고", "통계",
        "performance", "report", "PnL", "손익",
    ],
    "status_inquiry": [
        "포지션", "잔고", "상태", "현황", "보유", "종목",
        "조회", "알려줘", "어때",
    ],
    "strategy_setup": [
        "전략", "strategy", "적용", "설정", "세팅", "변경",
        "VB", "S1", "S2", "S3", "S4", "S5",
        "B1", "B2", "B3", "B4", "GR", "DS",
    ],
    "cancel_order": [
        "취소", "주문취소", "cancel", "철회", "주문 취소",
    ],
}

# Pre-compile intent regex for each group
_INTENT_RE: Dict[str, re.Pattern[str]] = {
    intent: re.compile("|".join(keywords), re.IGNORECASE)
    for intent, keywords in _INTENT_KEYWORDS.items()
}


# ============================================================================
# NLPParser
# ============================================================================

class NLPParser:
    """Lightweight rule-based Korean NLP parser for trading commands.

    This parser uses regex patterns and keyword matching -- no external NLP
    model required.  It is intentionally simple and fast so that it can run
    synchronously inside the MCP command pipeline with sub-millisecond
    latency.
    """

    def __init__(self) -> None:
        self._stock_aliases = STOCK_ALIASES
        self._code_to_name = _CODE_TO_NAME

    # ── Intent Classification ────────────────────────────────────────────

    def parse_intent(self, text: str) -> str:
        """Classify a Korean natural-language command into a known intent.

        Intent priority order (first match wins):
          1. cancel_order
          2. modify_stop_loss
          3. conditional_buy  (requires buy keyword + condition)
          4. conditional_sell (requires sell keyword + condition)
          5. performance_report
          6. status_inquiry
          7. strategy_setup
          8. unknown

        Args:
            text: Raw user input in Korean.

        Returns:
            One of the intent string constants, e.g. ``"conditional_buy"``.
        """
        if not text or not text.strip():
            return "unknown"

        normalized = text.strip()

        # cancel_order takes highest priority
        if _INTENT_RE["cancel_order"].search(normalized):
            return "cancel_order"

        # modify_stop_loss
        if _INTENT_RE["modify_stop_loss"].search(normalized):
            return "modify_stop_loss"

        # conditional_buy: buy keyword + some condition or quantity
        has_buy = _INTENT_RE["conditional_buy"].search(normalized)
        has_sell = _INTENT_RE["conditional_sell"].search(normalized)

        if has_buy and not has_sell:
            return "conditional_buy"
        if has_sell and not has_buy:
            return "conditional_sell"
        # If both buy and sell keywords appear, use word order
        if has_buy and has_sell:
            buy_pos = has_buy.start()
            sell_pos = has_sell.start()
            return "conditional_buy" if buy_pos > sell_pos else "conditional_sell"

        if _INTENT_RE["performance_report"].search(normalized):
            return "performance_report"

        if _INTENT_RE["status_inquiry"].search(normalized):
            return "status_inquiry"

        if _INTENT_RE["strategy_setup"].search(normalized):
            return "strategy_setup"

        return "unknown"

    # ── Entity Extraction ────────────────────────────────────────────────

    def extract_entities(self, text: str) -> Dict[str, object]:
        """Extract structured trading entities from a Korean command.

        Returned dictionary may include any of the following keys:

        - ``stock_code``  : 6-digit KRX stock code (str)
        - ``stock_name``  : Canonical Korean name (str)
        - ``quantity``    : Number of shares, or ``-1`` for "all" (int)
        - ``threshold``   : Percentage threshold for condition (float)
        - ``direction``   : ``"up"`` or ``"down"`` (str)
        - ``price``       : Absolute price in KRW (int)

        Args:
            text: Raw user input in Korean.

        Returns:
            Dictionary of extracted entities (may be empty).
        """
        if not text or not text.strip():
            return {}

        entities: Dict[str, object] = {}
        normalized = text.strip()

        # ── Stock identification ─────────────────────────────────────────
        stock_code, stock_name = self._find_stock(normalized)
        if stock_code:
            entities["stock_code"] = stock_code
        if stock_name:
            entities["stock_name"] = stock_name

        # ── Quantity ─────────────────────────────────────────────────────
        qty_match = _QTY_PATTERN.search(normalized)
        if qty_match:
            entities["quantity"] = int(qty_match.group("qty").replace(",", ""))
        elif _QTY_ALL_PATTERN.search(normalized):
            entities["quantity"] = -1  # sentinel: "all shares"

        # ── Condition patterns (percentage / absolute) ───────────────────
        for cp in CONDITION_PATTERNS:
            m = cp.pattern.search(normalized)
            if m:
                if cp.label == "price_up_pct":
                    entities["threshold"] = float(m.group("threshold"))
                    entities["direction"] = "up"
                elif cp.label == "price_down_pct":
                    entities["threshold"] = float(m.group("threshold"))
                    entities["direction"] = "down"
                elif cp.label == "price_above":
                    entities["price"] = int(m.group("price").replace(",", ""))
                    entities["direction"] = "up"
                elif cp.label == "price_below":
                    entities["price"] = int(m.group("price").replace(",", ""))
                    entities["direction"] = "down"
                break  # first match wins

        # ── Standalone price (e.g. stop-loss target) ─────────────────────
        if "price" not in entities:
            price_match = _PRICE_PATTERN.search(normalized)
            if price_match:
                entities["price"] = int(
                    price_match.group("price").replace(",", "")
                )

        logger.debug(
            "entities_extracted",
            raw_text=text[:80],
            entities=entities,
        )
        return entities

    # ── Stock Code Resolution ────────────────────────────────────────────

    def _resolve_stock_code(self, name: str) -> Optional[str]:
        """Look up a 6-digit stock code from a Korean stock name or alias.

        Args:
            name: Korean name, abbreviation, or 6-digit code string.

        Returns:
            The 6-digit stock code, or ``None`` if not found.
        """
        name = name.strip()

        # Already a 6-digit code
        if re.fullmatch(r"\d{6}", name):
            return name

        # Direct alias lookup
        if name in self._stock_aliases:
            return self._stock_aliases[name]

        # Case-insensitive search for English-style names (e.g. "NAVER")
        upper = name.upper()
        for alias, code in self._stock_aliases.items():
            if alias.upper() == upper:
                return code

        return None

    # ── Private Helpers ──────────────────────────────────────────────────

    def _find_stock(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        """Identify a stock from the text by scanning for known aliases.

        Returns:
            ``(stock_code, stock_name)`` or ``(None, None)`` if not found.
        """
        # Try longest alias first to avoid partial matches
        # e.g. "삼성전자" should beat "삼성"
        sorted_aliases = sorted(
            self._stock_aliases.keys(), key=len, reverse=True
        )
        for alias in sorted_aliases:
            if alias in text:
                code = self._stock_aliases[alias]
                canonical = self._code_to_name.get(code, alias)
                return code, canonical

        # Fallback: try a raw 6-digit code in the text
        code_match = _CODE_PATTERN.search(text)
        if code_match:
            code = code_match.group("code")
            name = self._code_to_name.get(code)
            return code, name

        return None, None
