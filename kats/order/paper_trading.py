"""
모의투자 엔진 (Paper Trading Engine).

RealtimeCache의 호가 데이터를 기반으로 가상 주문 체결을 시뮬레이션한다.
슬리피지, 시장 충격(market impact), 부분 체결을 모델링하여
실전 매매와 유사한 체결 환경을 제공한다.

체결 결과는 PaperAccount DB에 반영된다.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

import structlog

from kats.market.realtime_cache import OrderbookData, RealtimeCache

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# 즉시 체결 허용 비율: 주문 수량이 호가 최우선 잔량의 20% 이하일 때 즉시 체결
MAX_INSTANT_FILL_RATIO: float = 0.20

# 시장 충격 계수: fill_ratio 초과분에 비례한 추가 가격 영향
MARKET_IMPACT_COEFF: float = 0.05

# 기본 슬리피지 (%)
BASE_SLIPPAGE_PCT: float = 0.1


# ---------------------------------------------------------------------------
# PaperTradingEngine
# ---------------------------------------------------------------------------


class PaperTradingEngine:
    """
    모의투자 체결 엔진.

    RealtimeCache에서 실시간 호가를 읽어 가상 체결을 시뮬레이션한다.
    주문 수량 대비 호가 잔량 비율에 따라 즉시 전량 체결 또는
    부분 체결(시장 충격 포함)을 결정한다.

    Usage::

        engine = PaperTradingEngine(cache=realtime_cache)
        result = engine.execute_virtual_order({
            "order_type": "BUY",
            "stock_code": "005930",
            "quantity": 10,
            "price": 72000,
        })
    """

    def __init__(self, cache: RealtimeCache) -> None:
        self._cache = cache

        # 간이 모의 계좌 (in-memory)
        # 실제 운영에서는 DB PaperAccount 모델과 연동한다.
        self._paper_positions: Dict[str, Dict[str, Any]] = {}
        self._paper_cash: float = 0.0  # 초기 자본은 외부에서 설정

        logger.info(
            "paper_trading_engine_initialized",
            max_instant_fill_ratio=MAX_INSTANT_FILL_RATIO,
            market_impact_coeff=MARKET_IMPACT_COEFF,
            base_slippage_pct=BASE_SLIPPAGE_PCT,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute_virtual_order(
        self,
        order: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        가상 주문을 체결 시뮬레이션한다.

        1. RealtimeCache에서 호가 데이터를 조회한다.
        2. 매수(BUY)이면 매도호가(ask), 매도(SELL)이면 매수호가(bid)를 사용한다.
        3. fill_ratio = quantity / best_volume 을 계산한다.
        4. fill_ratio > 20%: 부분 체결 + 시장 충격 적용
           fill_ratio <= 20%: 전량 즉시 체결 + 기본 슬리피지 적용
        5. 모의 계좌를 갱신한다.

        Args:
            order: 주문 정보 딕셔너리.
                Required keys:
                - order_type: "BUY" 또는 "SELL"
                - stock_code: 종목코드 6자리
                - quantity: 주문 수량
                - price: 지정가 (참고용, 실제 체결은 호가 기반)

        Returns:
            체결 결과 딕셔너리:
            - success: bool
            - fill_type: "FULL_INSTANT" | "PARTIAL_SIMULATED"
            - fill_price: 체결가
            - fill_quantity: 체결 수량
            - remaining_quantity: 미체결 잔여 수량
            - slippage_pct: 슬리피지 비율 (%)
            - slippage_amount: 슬리피지 금액
            - market_impact_pct: 시장 충격 비율 (%)
            - requested_price: 원래 주문가
            - mode: "PAPER"
        """
        stock_code: str = order["stock_code"]
        order_type: str = order["order_type"]
        quantity: int = int(order["quantity"])
        requested_price: float = float(order.get("price", 0))

        # 호가 데이터 조회
        orderbook: Optional[OrderbookData] = self._cache.get_orderbook(stock_code)
        if orderbook is None:
            logger.warning(
                "paper_trade_no_orderbook",
                stock_code=stock_code,
                msg="호가 데이터 없음 -- 체결 불가",
            )
            return {
                "success": False,
                "fill_type": None,
                "fill_price": 0.0,
                "fill_quantity": 0,
                "remaining_quantity": quantity,
                "slippage_pct": 0.0,
                "slippage_amount": 0.0,
                "market_impact_pct": 0.0,
                "requested_price": requested_price,
                "mode": "PAPER",
                "error": "호가 데이터 없음",
            }

        # 매수: 매도호가(ask) 사용, 매도: 매수호가(bid) 사용
        if order_type == "BUY":
            best_price = orderbook.ask_prices[0] if orderbook.ask_prices else 0.0
            best_volume = orderbook.ask_volumes[0] if orderbook.ask_volumes else 0
        else:
            best_price = orderbook.bid_prices[0] if orderbook.bid_prices else 0.0
            best_volume = orderbook.bid_volumes[0] if orderbook.bid_volumes else 0

        if best_price <= 0 or best_volume <= 0:
            logger.warning(
                "paper_trade_invalid_orderbook",
                stock_code=stock_code,
                best_price=best_price,
                best_volume=best_volume,
                msg="호가 가격/수량 0 -- 체결 불가",
            )
            return {
                "success": False,
                "fill_type": None,
                "fill_price": 0.0,
                "fill_quantity": 0,
                "remaining_quantity": quantity,
                "slippage_pct": 0.0,
                "slippage_amount": 0.0,
                "market_impact_pct": 0.0,
                "requested_price": requested_price,
                "mode": "PAPER",
                "error": "호가 가격/수량 부적합",
            }

        # fill_ratio 계산
        fill_ratio: float = quantity / best_volume

        if fill_ratio > MAX_INSTANT_FILL_RATIO:
            # ----------------------------------------------------------
            # 부분 체결 시뮬레이션 + 시장 충격
            # ----------------------------------------------------------
            fill_quantity = int(best_volume * MAX_INSTANT_FILL_RATIO)
            if fill_quantity < 1:
                fill_quantity = 1
            remaining_quantity = quantity - fill_quantity

            # 시장 충격: fill_ratio 초과분에 비례
            market_impact_pct = (fill_ratio - MAX_INSTANT_FILL_RATIO) * MARKET_IMPACT_COEFF * 100
            total_slippage_pct = BASE_SLIPPAGE_PCT + market_impact_pct

            # 매수: 가격 상승, 매도: 가격 하락
            if order_type == "BUY":
                fill_price = best_price * (1 + total_slippage_pct / 100)
            else:
                fill_price = best_price * (1 - total_slippage_pct / 100)

            fill_type = "PARTIAL_SIMULATED"

            logger.info(
                "paper_trade_partial_fill",
                stock_code=stock_code,
                order_type=order_type,
                requested_qty=quantity,
                fill_qty=fill_quantity,
                remaining_qty=remaining_quantity,
                fill_ratio=round(fill_ratio, 4),
                best_price=best_price,
                fill_price=round(fill_price, 2),
                market_impact_pct=round(market_impact_pct, 4),
                total_slippage_pct=round(total_slippage_pct, 4),
            )
        else:
            # ----------------------------------------------------------
            # 전량 즉시 체결 + 기본 슬리피지
            # ----------------------------------------------------------
            fill_quantity = quantity
            remaining_quantity = 0
            market_impact_pct = 0.0
            total_slippage_pct = BASE_SLIPPAGE_PCT

            if order_type == "BUY":
                fill_price = best_price * (1 + BASE_SLIPPAGE_PCT / 100)
            else:
                fill_price = best_price * (1 - BASE_SLIPPAGE_PCT / 100)

            fill_type = "FULL_INSTANT"

            logger.info(
                "paper_trade_full_fill",
                stock_code=stock_code,
                order_type=order_type,
                quantity=quantity,
                fill_ratio=round(fill_ratio, 4),
                best_price=best_price,
                fill_price=round(fill_price, 2),
                slippage_pct=BASE_SLIPPAGE_PCT,
            )

        # 슬리피지 금액 계산
        slippage_amount = abs(fill_price - best_price) * fill_quantity

        # 모의 계좌 갱신
        self._update_paper_account(order, fill_price, fill_quantity)

        return {
            "success": True,
            "fill_type": fill_type,
            "fill_price": round(fill_price, 2),
            "fill_quantity": fill_quantity,
            "remaining_quantity": remaining_quantity,
            "slippage_pct": round(total_slippage_pct, 4),
            "slippage_amount": round(slippage_amount, 2),
            "market_impact_pct": round(market_impact_pct, 4),
            "requested_price": requested_price,
            "mode": "PAPER",
            "timestamp": time.time(),
        }

    # ------------------------------------------------------------------
    # Paper account management
    # ------------------------------------------------------------------

    def _update_paper_account(
        self,
        order: Dict[str, Any],
        fill_price: float,
        fill_quantity: int,
    ) -> None:
        """
        모의 계좌 포지션 및 현금을 갱신한다.

        매수 시:
          - 현금 차감, 포지션 증가, 평균단가 재계산
        매도 시:
          - 현금 증가, 포지션 감소, 실현손익 계산

        Args:
            order: 주문 정보 딕셔너리.
            fill_price: 체결가.
            fill_quantity: 체결 수량.
        """
        stock_code: str = order["stock_code"]
        order_type: str = order["order_type"]
        trade_amount = fill_price * fill_quantity

        position = self._paper_positions.get(stock_code, {
            "stock_code": stock_code,
            "quantity": 0,
            "avg_price": 0.0,
            "total_cost": 0.0,
            "realized_pnl": 0.0,
        })

        if order_type == "BUY":
            # 매수: 포지션 증가
            old_qty = position["quantity"]
            old_cost = position["total_cost"]
            new_qty = old_qty + fill_quantity
            new_cost = old_cost + trade_amount
            position["quantity"] = new_qty
            position["total_cost"] = new_cost
            position["avg_price"] = new_cost / new_qty if new_qty > 0 else 0.0
            self._paper_cash -= trade_amount

            logger.debug(
                "paper_account_buy",
                stock_code=stock_code,
                fill_price=fill_price,
                fill_quantity=fill_quantity,
                new_qty=new_qty,
                avg_price=round(position["avg_price"], 2),
                remaining_cash=round(self._paper_cash, 0),
            )

        elif order_type == "SELL":
            # 매도: 포지션 감소 + 실현 손익
            avg_price = position["avg_price"]
            pnl = (fill_price - avg_price) * fill_quantity
            position["quantity"] -= fill_quantity
            position["realized_pnl"] += pnl
            if position["quantity"] > 0:
                position["total_cost"] = position["avg_price"] * position["quantity"]
            else:
                position["total_cost"] = 0.0
                position["avg_price"] = 0.0
            self._paper_cash += trade_amount

            logger.debug(
                "paper_account_sell",
                stock_code=stock_code,
                fill_price=fill_price,
                fill_quantity=fill_quantity,
                remaining_qty=position["quantity"],
                pnl=round(pnl, 0),
                total_realized_pnl=round(position["realized_pnl"], 0),
                remaining_cash=round(self._paper_cash, 0),
            )

        self._paper_positions[stock_code] = position

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def get_paper_position(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """특정 종목의 모의 포지션을 조회한다."""
        return self._paper_positions.get(stock_code)

    def get_all_paper_positions(self) -> Dict[str, Dict[str, Any]]:
        """전체 모의 포지션을 반환한다."""
        return dict(self._paper_positions)

    @property
    def paper_cash(self) -> float:
        """모의 계좌 현금 잔고."""
        return self._paper_cash

    @paper_cash.setter
    def paper_cash(self, value: float) -> None:
        """모의 계좌 현금을 설정한다 (초기화 시 사용)."""
        self._paper_cash = value
        logger.info("paper_cash_set", cash=value)

    def get_total_equity(self) -> float:
        """
        모의 계좌 총 평가금을 계산한다.

        현금 + SUM(각 포지션의 현재가 * 보유수량)
        현재가는 RealtimeCache에서 조회한다.
        """
        equity = self._paper_cash

        for stock_code, position in self._paper_positions.items():
            qty = position["quantity"]
            if qty <= 0:
                continue

            price_data = self._cache.get_price(stock_code)
            if price_data is not None:
                equity += price_data.price * qty
            else:
                # 현재가 없으면 평균단가로 대체
                equity += position["avg_price"] * qty

        return equity

    def reset(self, initial_cash: float = 0.0) -> None:
        """모의 계좌를 초기화한다."""
        self._paper_positions.clear()
        self._paper_cash = initial_cash
        logger.info("paper_trading_engine_reset", initial_cash=initial_cash)

    def __repr__(self) -> str:
        pos_count = sum(
            1 for p in self._paper_positions.values() if p["quantity"] > 0
        )
        return (
            f"PaperTradingEngine(positions={pos_count}, "
            f"cash={self._paper_cash:,.0f})"
        )
