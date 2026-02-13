"""
피라미딩 관리자 (Pyramid Manager).

수익이 발생한 포지션에 대해 단계적으로 추가 매수(피라미딩)를 관리한다.
각 단계마다 수량을 줄여 역피라미드 형태로 포지션을 확대하며,
지정된 수익률 트리거에 도달해야만 다음 단계로 진행할 수 있다.

설계 원칙:
- 최대 3단계 피라미딩 (기본 설정)
- 단계별 비중: 50% -> 30% -> 20% (역피라미드)
- 수익률 트리거: 0% -> 5% -> 10% (이전 단계 대비)
- 손실 중인 포지션에는 절대 추가 매수하지 않는다
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# PyramidConfig
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PyramidConfig:
    """
    피라미딩 설정.

    Attributes:
        max_stages: 최대 피라미딩 단계 수 (초기 진입 포함).
        stage_ratios: 각 단계별 총 포지션 대비 비중.
                      합계는 1.0이어야 한다.
        profit_trigger_pct: 각 단계 진입을 위한 최소 수익률 (%).
                            첫 번째 값은 0 (초기 진입).
    """

    max_stages: int = 3
    stage_ratios: tuple[float, ...] = (0.5, 0.3, 0.2)
    profit_trigger_pct: tuple[float, ...] = (0.0, 5.0, 10.0)

    def __post_init__(self) -> None:
        if len(self.stage_ratios) != self.max_stages:
            raise ValueError(
                f"stage_ratios 길이({len(self.stage_ratios)})가 "
                f"max_stages({self.max_stages})와 일치하지 않습니다."
            )
        if len(self.profit_trigger_pct) != self.max_stages:
            raise ValueError(
                f"profit_trigger_pct 길이({len(self.profit_trigger_pct)})가 "
                f"max_stages({self.max_stages})와 일치하지 않습니다."
            )
        ratio_sum = sum(self.stage_ratios)
        if not (0.99 <= ratio_sum <= 1.01):
            raise ValueError(
                f"stage_ratios 합계({ratio_sum:.2f})가 1.0이 아닙니다."
            )


# ---------------------------------------------------------------------------
# Default config
# ---------------------------------------------------------------------------

DEFAULT_PYRAMID_CONFIG = PyramidConfig(
    max_stages=3,
    stage_ratios=(0.5, 0.3, 0.2),
    profit_trigger_pct=(0.0, 5.0, 10.0),
)


# ---------------------------------------------------------------------------
# PyramidManager
# ---------------------------------------------------------------------------


class PyramidManager:
    """
    피라미딩 관리자.

    수익 중인 포지션에 대해 단계적 추가 매수 기회를 판단하고,
    각 trade_id별 피라미딩 진행 상태를 추적한다.

    Usage::

        pm = PyramidManager()
        opportunity = pm.check_pyramid_opportunity(position, current_price=75000)
        if opportunity:
            # 추가 매수 주문 실행
            order_manager.place_order(signal_from_opportunity)
            pm.record_stage_execution(trade_id, stage=1, ...)
    """

    def __init__(
        self,
        config: Optional[PyramidConfig] = None,
    ) -> None:
        self._config = config or DEFAULT_PYRAMID_CONFIG
        # trade_id -> pyramid state tracking
        self._pyramid_state: Dict[str, Dict[str, Any]] = {}

        logger.info(
            "pyramid_manager_initialized",
            max_stages=self._config.max_stages,
            stage_ratios=self._config.stage_ratios,
            profit_trigger_pct=self._config.profit_trigger_pct,
        )

    # ------------------------------------------------------------------
    # Pyramid opportunity check
    # ------------------------------------------------------------------

    def check_pyramid_opportunity(
        self,
        position: Dict[str, Any],
        current_price: float,
    ) -> Optional[Dict[str, Any]]:
        """
        피라미딩 추가 매수 기회를 판단한다.

        조건:
        1. 현재 포지션이 수익 중이어야 한다 (current_price > avg_entry_price).
        2. 아직 max_stages에 도달하지 않았어야 한다.
        3. 현재 수익률이 다음 단계의 profit_trigger_pct를 충족해야 한다.
        4. 다음 단계의 수량은 이전 단계보다 작아야 한다 (역피라미드).

        Args:
            position: 포지션 정보 딕셔너리.
                Required keys:
                - trade_id: 거래 식별자
                - stock_code: 종목코드
                - avg_entry_price: 평균 진입가
                - quantity: 현재 보유 수량
                - total_planned_quantity: 계획된 총 수량
                - order_type: "BUY" (매수 포지션만 피라미딩 가능)
            current_price: 현재 시장가.

        Returns:
            피라미딩 기회가 있으면 추가 매수 정보 딕셔너리, 없으면 None.
            - stage: 다음 피라미딩 단계 번호 (1-indexed)
            - stock_code: 종목코드
            - quantity: 추가 매수 수량
            - ratio: 이번 단계 비중
            - trigger_pct: 충족된 수익률 트리거 (%)
            - current_profit_pct: 현재 수익률 (%)
            - reason: 피라미딩 사유
        """
        trade_id: str = position.get("trade_id", "")
        stock_code: str = position.get("stock_code", "")
        avg_entry_price: float = float(position.get("avg_entry_price", 0))
        order_type: str = position.get("order_type", "BUY")
        total_planned_qty: int = int(position.get("total_planned_quantity", 0))

        # 매수 포지션만 피라미딩
        if order_type != "BUY":
            logger.debug(
                "pyramid_skip_not_buy",
                trade_id=trade_id,
                order_type=order_type,
            )
            return None

        if avg_entry_price <= 0 or total_planned_qty <= 0:
            logger.debug(
                "pyramid_skip_invalid_position",
                trade_id=trade_id,
                avg_entry_price=avg_entry_price,
                total_planned_qty=total_planned_qty,
            )
            return None

        # 현재 수익률 계산
        profit_pct = ((current_price - avg_entry_price) / avg_entry_price) * 100

        # 손실 중이면 피라미딩 금지
        if profit_pct <= 0:
            logger.debug(
                "pyramid_skip_in_loss",
                trade_id=trade_id,
                stock_code=stock_code,
                profit_pct=round(profit_pct, 2),
            )
            return None

        # 현재 피라미딩 단계 확인
        state = self._pyramid_state.get(trade_id, {
            "current_stage": 0,
            "stages_executed": [],
        })
        current_stage: int = state["current_stage"]
        next_stage: int = current_stage + 1

        # 최대 단계 도달 확인
        if next_stage >= self._config.max_stages:
            logger.debug(
                "pyramid_skip_max_stages",
                trade_id=trade_id,
                stock_code=stock_code,
                current_stage=current_stage,
                max_stages=self._config.max_stages,
            )
            return None

        # 다음 단계 수익률 트리거 확인
        trigger_pct = self._config.profit_trigger_pct[next_stage]
        if profit_pct < trigger_pct:
            logger.debug(
                "pyramid_skip_trigger_not_met",
                trade_id=trade_id,
                stock_code=stock_code,
                current_profit_pct=round(profit_pct, 2),
                required_trigger_pct=trigger_pct,
                next_stage=next_stage,
            )
            return None

        # 다음 단계 수량 계산
        stage_ratio = self._config.stage_ratios[next_stage]
        additional_qty = max(1, int(total_planned_qty * stage_ratio))

        opportunity = {
            "stage": next_stage,
            "stock_code": stock_code,
            "trade_id": trade_id,
            "quantity": additional_qty,
            "ratio": stage_ratio,
            "trigger_pct": trigger_pct,
            "current_profit_pct": round(profit_pct, 2),
            "current_price": current_price,
            "avg_entry_price": avg_entry_price,
            "reason": (
                f"피라미딩 {next_stage + 1}단계: 수익률 {profit_pct:.1f}% >= "
                f"트리거 {trigger_pct:.1f}%, 추가 {additional_qty}주 "
                f"(비중 {stage_ratio * 100:.0f}%)"
            ),
        }

        logger.info(
            "pyramid_opportunity_found",
            trade_id=trade_id,
            stock_code=stock_code,
            stage=next_stage,
            profit_pct=round(profit_pct, 2),
            trigger_pct=trigger_pct,
            additional_qty=additional_qty,
            ratio=stage_ratio,
        )

        return opportunity

    # ------------------------------------------------------------------
    # Stage execution recording
    # ------------------------------------------------------------------

    def record_stage_execution(
        self,
        trade_id: str,
        stage: int,
        fill_price: float,
        fill_quantity: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        피라미딩 단계 실행을 기록한다.

        Args:
            trade_id: 거래 식별자.
            stage: 실행된 피라미딩 단계.
            fill_price: 체결가.
            fill_quantity: 체결 수량.
            metadata: 추가 메타데이터.

        Returns:
            갱신된 피라미딩 상태.
        """
        import time

        state = self._pyramid_state.get(trade_id, {
            "current_stage": 0,
            "stages_executed": [],
        })

        stage_record = {
            "stage": stage,
            "fill_price": fill_price,
            "fill_quantity": fill_quantity,
            "timestamp": time.time(),
            "metadata": metadata,
        }

        state["current_stage"] = stage
        state["stages_executed"].append(stage_record)
        self._pyramid_state[trade_id] = state

        logger.info(
            "pyramid_stage_recorded",
            trade_id=trade_id,
            stage=stage,
            fill_price=fill_price,
            fill_quantity=fill_quantity,
            total_stages_executed=len(state["stages_executed"]),
        )

        return state

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def get_pyramid_stage(self, trade_id: str) -> Dict[str, Any]:
        """
        특정 거래의 피라미딩 단계 정보를 조회한다.

        Args:
            trade_id: 거래 식별자.

        Returns:
            피라미딩 상태 딕셔너리:
            - current_stage: 현재 단계 (0=초기 진입만 완료)
            - stages_executed: 실행된 단계 기록 리스트
            - max_stages: 최대 허용 단계
            - remaining_stages: 남은 피라미딩 가능 횟수
            - config: 적용된 PyramidConfig 정보
        """
        state = self._pyramid_state.get(trade_id, {
            "current_stage": 0,
            "stages_executed": [],
        })

        current_stage = state["current_stage"]
        remaining = max(0, self._config.max_stages - 1 - current_stage)

        return {
            "trade_id": trade_id,
            "current_stage": current_stage,
            "stages_executed": list(state["stages_executed"]),
            "max_stages": self._config.max_stages,
            "remaining_stages": remaining,
            "config": {
                "max_stages": self._config.max_stages,
                "stage_ratios": list(self._config.stage_ratios),
                "profit_trigger_pct": list(self._config.profit_trigger_pct),
            },
        }

    def get_all_pyramid_states(self) -> Dict[str, Dict[str, Any]]:
        """등록된 모든 피라미딩 상태를 반환한다."""
        return {
            trade_id: self.get_pyramid_stage(trade_id)
            for trade_id in self._pyramid_state
        }

    def has_pyramid_in_progress(self, trade_id: str) -> bool:
        """해당 거래에 피라미딩이 진행 중인지 확인한다."""
        state = self._pyramid_state.get(trade_id)
        if state is None:
            return False
        return state["current_stage"] < self._config.max_stages - 1

    def remove_trade(self, trade_id: str) -> None:
        """완료된 거래의 피라미딩 상태를 제거한다."""
        removed = self._pyramid_state.pop(trade_id, None)
        if removed is not None:
            logger.info(
                "pyramid_state_removed",
                trade_id=trade_id,
                stages_executed=len(removed.get("stages_executed", [])),
            )

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------

    @property
    def config(self) -> PyramidConfig:
        """현재 피라미딩 설정."""
        return self._config

    def __repr__(self) -> str:
        active = sum(
            1 for s in self._pyramid_state.values()
            if s["current_stage"] < self._config.max_stages - 1
        )
        return (
            f"PyramidManager(active_pyramids={active}, "
            f"max_stages={self._config.max_stages})"
        )
