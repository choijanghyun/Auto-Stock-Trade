"""
KATS (KIS Auto Trading System) v1.1
엔트리포인트 및 스케줄러

일일 스케줄:
- 06:30: 시스템 기동 + 헬스체크 + Redis 연결 확인
- 08:00: 토큰 갱신 확인
- 08:30: 종목 스캐닝 (StockScreener.scan_daily) — REST 사용
- 08:50: 전략 선택 + 매매 시나리오 준비
- 09:00: WebSocket 구독 시작 + VI모니터 기동 + OrderTracker 시작
- 09:00~15:30: 실시간 매매 루프 (시세=캐시 읽기, 주문=REST)
- 15:30: 장 마감 — OrderTracker 미체결 전량 취소
- 15:40: Redis → DB Bulk Insert + 일간 통계 집계
- 16:00: 성과 리포트 생성 + 알림 전송
- 16:30: WebSocket 연결 종료 + Redis 캐시 초기화
- 매주 금요일 16:00: 주간 리뷰 생성
- 매월 말 16:00: 월간 리뷰 + SQN 산출
"""

import asyncio
import signal
import sys
from datetime import datetime, date, timedelta
from typing import Optional, List

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from kats.config.settings import Settings
from kats.utils.logger import setup_logging, get_logger
from kats.auth.token_manager import TokenManager
from kats.auth.hashkey_manager import HashkeyManager
from kats.api.rate_limiter import RateLimiter
from kats.api.kis_rest_client import KISRestClient
from kats.api.kis_websocket_client import KISWebSocketClient
from kats.market.realtime_cache import RealtimeCache
from kats.market.vi_monitor import VIMonitor
from kats.market.data_hub import MarketDataHub
from kats.market.stock_screener import StockScreener
from kats.market.indicator_calculator import IndicatorCalculator
from kats.market.sector_analyzer import SectorAnalyzer
from kats.strategy.strategy_selector import StrategySelector
from kats.strategy.base_strategy import MarketRegime, TradeSignal
from kats.risk.risk_manager import RiskManager
from kats.risk.position_sizer import PositionSizer
from kats.risk.trailing_stop import TrailingStop
from kats.risk.daily_kill_switch import DailyKillSwitch
from kats.risk.drawdown_protocol import DrawdownProtocol
from kats.risk.grade_allocator import GradeAllocator
from kats.risk.global_position_lock import GlobalPositionLock
from kats.risk.margin_guard import MarginGuard
from kats.order.order_manager import OrderManager
from kats.order.order_state_machine import OrderStateMachine
from kats.order.order_tracker import OrderTracker
from kats.order.paper_trading import PaperTradingEngine
from kats.order.pyramid_manager import PyramidManager
from kats.journal.trade_journal import TradeJournal
from kats.journal.performance_analyzer import PerformanceAnalyzer
from kats.journal.review_generator import ReviewGenerator
from kats.notification.notifier import NotificationService
from kats.notification.approval_gateway import ApprovalGateway
from kats.ai.mcp_handler import MCPHandler
from kats.ai.nlp_parser import NLPParser
from kats.database.repository import Repository
from kats.database.redis_buffer import RedisTickBuffer
from kats.utils.event_calendar import EventCalendar
from kats.utils.market_regime import MarketRegimeDetector

logger = get_logger("kats.main")


class KATSSystem:
    """KATS 자동매매 시스템 메인 클래스"""

    def __init__(self):
        self.settings = Settings
        self.scheduler = AsyncIOScheduler(timezone="Asia/Seoul")
        self._running = False

        # 컴포넌트 (init_components에서 초기화)
        self.token_manager: Optional[TokenManager] = None
        self.hashkey_manager: Optional[HashkeyManager] = None
        self.rate_limiter: Optional[RateLimiter] = None
        self.rest_client: Optional[KISRestClient] = None
        self.ws_client: Optional[KISWebSocketClient] = None
        self.cache: Optional[RealtimeCache] = None
        self.vi_monitor: Optional[VIMonitor] = None
        self.data_hub: Optional[MarketDataHub] = None
        self.screener: Optional[StockScreener] = None
        self.strategy_selector: Optional[StrategySelector] = None
        self.risk_manager: Optional[RiskManager] = None
        self.order_manager: Optional[OrderManager] = None
        self.journal: Optional[TradeJournal] = None
        self.analyzer: Optional[PerformanceAnalyzer] = None
        self.reviewer: Optional[ReviewGenerator] = None
        self.notifier: Optional[NotificationService] = None
        self.repository: Optional[Repository] = None
        self.redis_buffer: Optional[RedisTickBuffer] = None
        self.event_calendar: Optional[EventCalendar] = None
        self.regime_detector: Optional[MarketRegimeDetector] = None

        # 상태
        self.current_regime: MarketRegime = MarketRegime.SIDEWAYS
        self.daily_candidates: List = []
        self.active_strategies: List = []

    async def init_components(self):
        """모든 컴포넌트 초기화"""
        logger.info("시스템 컴포넌트 초기화 시작")

        # 1. 데이터베이스
        self.repository = Repository(self.settings.DB_URL)
        await self.repository.init_db()

        # 2. Redis 버퍼
        self.redis_buffer = RedisTickBuffer(self.settings.REDIS_URL)

        # 3. 인증
        base_url = self.settings.get_base_url()
        self.token_manager = TokenManager(
            app_key=self.settings.KIS_APP_KEY,
            app_secret=self.settings.KIS_APP_SECRET,
            base_url=base_url,
        )
        self.hashkey_manager = HashkeyManager(
            app_key=self.settings.KIS_APP_KEY,
            app_secret=self.settings.KIS_APP_SECRET,
            base_url=base_url,
        )

        # 4. API 클라이언트
        self.rate_limiter = RateLimiter(calls_per_second=18)
        self.rest_client = KISRestClient(
            token_manager=self.token_manager,
            hashkey_manager=self.hashkey_manager,
            rate_limiter=self.rate_limiter,
            mode=self.settings.TRADE_MODE,
            account_no=self.settings.KIS_ACCOUNT_NO,
            account_product_code=self.settings.KIS_ACCOUNT_PROD_CODE,
        )

        # 5. 실시간 데이터
        self.cache = RealtimeCache()
        self.vi_monitor = VIMonitor(cache=self.cache)
        self.data_hub = MarketDataHub(
            cache=self.cache,
            vi_monitor=self.vi_monitor,
            redis_buffer=self.redis_buffer,
            rest_client=self.rest_client,
        )

        # 6. 종목 스크리너
        self.screener = StockScreener(rest_client=self.rest_client)

        # 7. 전략 선택기
        self.strategy_selector = StrategySelector()
        self.regime_detector = MarketRegimeDetector()

        # 8. 리스크 관리
        position_sizer = PositionSizer()
        grade_allocator = GradeAllocator()
        global_lock = GlobalPositionLock()
        margin_guard = MarginGuard(rest_client=self.rest_client)
        kill_switch = DailyKillSwitch(
            daily_loss_limit_pct=self.settings.DAILY_LOSS_LIMIT * 100
        )
        drawdown = DrawdownProtocol(
            notifier=None,  # 아래에서 설정
            repository=self.repository,
        )

        self.risk_manager = RiskManager(
            position_sizer=position_sizer,
            grade_allocator=grade_allocator,
            global_lock=global_lock,
            vi_monitor=self.vi_monitor,
            margin_guard=margin_guard,
            kill_switch=kill_switch,
            drawdown_protocol=drawdown,
            event_calendar=None,  # 아래에서 설정
        )

        # 9. 주문 관리
        state_machine = OrderStateMachine()
        paper_engine = PaperTradingEngine(cache=self.cache)
        pyramid_manager = PyramidManager()
        order_tracker = OrderTracker(
            state_machine=state_machine,
            rest_client=self.rest_client,
        )

        self.order_manager = OrderManager(
            rest_client=self.rest_client,
            state_machine=state_machine,
            order_tracker=order_tracker,
            paper_engine=paper_engine,
            risk_manager=self.risk_manager,
            margin_guard=margin_guard,
            pyramid_manager=pyramid_manager,
            trade_mode=self.settings.TRADE_MODE,
        )

        # 10. 매매 일지 및 성과 분석
        self.analyzer = PerformanceAnalyzer(repository=self.repository)
        self.journal = TradeJournal(repository=self.repository)
        self.reviewer = ReviewGenerator(
            performance_analyzer=self.analyzer,
            repository=self.repository,
        )

        # 11. 알림
        self.notifier = NotificationService(
            slack_webhook_url=self.settings.SLACK_WEBHOOK_URL,
            telegram_bot_token=self.settings.TELEGRAM_BOT_TOKEN,
            telegram_chat_id=self.settings.TELEGRAM_CHAT_ID,
        )

        # 순환 참조 해결
        drawdown.notifier = self.notifier
        kill_switch.notifier = self.notifier
        kill_switch.order_manager = self.order_manager

        # 12. 이벤트 캘린더
        self.event_calendar = EventCalendar(repository=self.repository)
        self.risk_manager.event_calendar = self.event_calendar

        # 13. AI/NLP (선택적)
        nlp_parser = NLPParser()
        approval_gateway = ApprovalGateway(notifier=self.notifier)
        self.mcp_handler = MCPHandler(
            nlp_parser=nlp_parser,
            approval_gateway=approval_gateway,
            order_manager=self.order_manager,
            performance_analyzer=self.analyzer,
        )

        logger.info(
            "시스템 컴포넌트 초기화 완료",
            trade_mode=self.settings.TRADE_MODE,
            total_capital=f"{self.settings.TOTAL_CAPITAL:,}원",
        )

    def setup_scheduler(self):
        """일일 스케줄 설정"""
        # 06:30 시스템 헬스체크
        self.scheduler.add_job(
            self.health_check,
            CronTrigger(hour=6, minute=30),
            id="health_check",
            name="시스템 헬스체크",
        )

        # 08:00 토큰 갱신
        self.scheduler.add_job(
            self.refresh_token,
            CronTrigger(hour=8, minute=0),
            id="refresh_token",
            name="토큰 갱신",
        )

        # 08:30 종목 스캐닝
        self.scheduler.add_job(
            self.scan_stocks,
            CronTrigger(hour=8, minute=30, day_of_week="mon-fri"),
            id="scan_stocks",
            name="종목 스캐닝",
        )

        # 08:50 전략 선택
        self.scheduler.add_job(
            self.prepare_strategies,
            CronTrigger(hour=8, minute=50, day_of_week="mon-fri"),
            id="prepare_strategies",
            name="전략 선택 및 준비",
        )

        # 09:00 매매 시작
        self.scheduler.add_job(
            self.start_trading,
            CronTrigger(hour=9, minute=0, day_of_week="mon-fri"),
            id="start_trading",
            name="매매 시작",
        )

        # 15:30 장 마감 처리
        self.scheduler.add_job(
            self.market_close,
            CronTrigger(hour=15, minute=30, day_of_week="mon-fri"),
            id="market_close",
            name="장 마감 처리",
        )

        # 15:40 Redis → DB Flush
        self.scheduler.add_job(
            self.flush_redis_to_db,
            CronTrigger(hour=15, minute=40, day_of_week="mon-fri"),
            id="flush_redis",
            name="Redis → DB Bulk Insert",
        )

        # 16:00 일간 성과 리포트
        self.scheduler.add_job(
            self.generate_daily_report,
            CronTrigger(hour=16, minute=0, day_of_week="mon-fri"),
            id="daily_report",
            name="일간 성과 리포트",
        )

        # 16:30 정리
        self.scheduler.add_job(
            self.cleanup,
            CronTrigger(hour=16, minute=30, day_of_week="mon-fri"),
            id="cleanup",
            name="일일 정리",
        )

        # 매주 금요일 16:00 주간 리뷰
        self.scheduler.add_job(
            self.generate_weekly_review,
            CronTrigger(hour=16, minute=0, day_of_week="fri"),
            id="weekly_review",
            name="주간 리뷰",
        )

        # 매월 말일 16:00 월간 리뷰
        self.scheduler.add_job(
            self.generate_monthly_review,
            CronTrigger(hour=16, minute=0, day="last", day_of_week="mon-fri"),
            id="monthly_review",
            name="월간 리뷰",
        )

        logger.info("스케줄러 설정 완료")

    # ===== 스케줄 작업 =====

    async def health_check(self):
        """06:30 시스템 헬스체크"""
        logger.info("=== 시스템 헬스체크 시작 ===")
        try:
            # API 연결 확인
            token = await self.token_manager.get_token()
            if token:
                logger.info("API 토큰 유효")
            else:
                logger.warning("API 토큰 없음 - 갱신 필요")

            # Redis 연결 확인
            try:
                await self.redis_buffer.redis.ping()
                logger.info("Redis 연결 정상")
            except Exception as e:
                logger.warning("Redis 연결 실패", error=str(e))

            # DB 연결 확인
            try:
                config = await self.repository.get_system_config("trade_mode")
                logger.info("DB 연결 정상", trade_mode=config)
            except Exception as e:
                logger.warning("DB 연결 실패", error=str(e))

            logger.info("=== 헬스체크 완료 ===")
        except Exception as e:
            logger.error("헬스체크 실패", error=str(e))
            await self.notifier.send_critical(f"시스템 헬스체크 실패: {e}")

    async def refresh_token(self):
        """08:00 토큰 갱신"""
        try:
            token = await self.token_manager.get_token()
            logger.info("토큰 갱신 완료")
        except Exception as e:
            logger.error("토큰 갱신 실패", error=str(e))
            await self.notifier.send_critical(f"토큰 갱신 실패: {e}")

    async def scan_stocks(self):
        """08:30 종목 스캐닝"""
        logger.info("=== 종목 스캐닝 시작 ===")
        try:
            self.daily_candidates = await self.screener.scan_daily()
            logger.info(
                "종목 스캐닝 완료",
                candidate_count=len(self.daily_candidates),
                candidates=[c.stock_code for c in self.daily_candidates],
            )

            # 히스토리컬 데이터 로드
            for candidate in self.daily_candidates:
                await self.data_hub.load_historical_data(candidate.stock_code)

        except Exception as e:
            logger.error("종목 스캐닝 실패", error=str(e))
            await self.notifier.send_critical(f"종목 스캐닝 실패: {e}")

    async def prepare_strategies(self):
        """08:50 전략 선택 및 시나리오 준비"""
        logger.info("=== 전략 선택 시작 ===")
        try:
            # 장세 판단
            kospi_data = await self.data_hub.get_kospi_data()
            self.current_regime = self.regime_detector.detect(kospi_data)

            # 전략 선택
            self.active_strategies = self.strategy_selector.select_strategies(
                self.current_regime
            )

            # Kill Switch 일일 초기화
            balance = await self.rest_client.get_balance()
            total_equity = float(
                balance.get("output2", [{}])[0].get("tot_evlu_amt", self.settings.TOTAL_CAPITAL)
            )
            self.risk_manager.kill_switch.reset_daily(total_equity)

            logger.info(
                "전략 선택 완료",
                regime=self.current_regime.value,
                strategies=[s.strategy_code for s in self.active_strategies],
            )

            # 이벤트 확인
            events = await self.event_calendar.get_upcoming_events(days_ahead=1)
            if events:
                for event in events:
                    await self.notifier.send_event_alert(event)
                    logger.info("특수 이벤트 감지", event=event)

        except Exception as e:
            logger.error("전략 선택 실패", error=str(e))

    async def start_trading(self):
        """09:00 실시간 매매 시작"""
        logger.info("=== 매매 시작 ===", mode=self.settings.TRADE_MODE)
        try:
            # WebSocket 연결 및 구독
            approval_key = await self.token_manager.get_approval_key()
            self.ws_client = KISWebSocketClient(approval_key=approval_key)

            # WebSocket 콜백 등록
            self.data_hub.register_websocket_callbacks(self.ws_client)

            # 종목 구독
            for candidate in self.daily_candidates:
                code = candidate.stock_code
                await self.ws_client.subscribe_execution(code)
                await self.ws_client.subscribe_orderbook(code)
                await self.ws_client.subscribe_vi(code)

            # 주문 체결 통보 구독
            await self.ws_client.subscribe_order_notice()

            # WebSocket 연결 시작 (백그라운드)
            asyncio.create_task(self.ws_client.connect())

            # OrderTracker 시작
            asyncio.create_task(self.order_manager.order_tracker.start_tracking())

            # 실시간 매매 루프 시작
            asyncio.create_task(self._trading_loop())

            logger.info("매매 시스템 기동 완료")

        except Exception as e:
            logger.error("매매 시작 실패", error=str(e))
            await self.notifier.send_critical(f"매매 시작 실패: {e}")

    async def _trading_loop(self):
        """실시간 매매 루프 (09:00~15:30)"""
        logger.info("매매 루프 시작")

        # 09:00~09:15 관찰 전용 (매수 금지)
        observation_end = datetime.now().replace(hour=9, minute=15, second=0)
        while datetime.now() < observation_end:
            await asyncio.sleep(1)

        market_close_time = datetime.now().replace(hour=15, minute=30, second=0)

        while datetime.now() < market_close_time and self._running:
            try:
                # Kill Switch 확인
                if self.risk_manager.kill_switch.is_killed:
                    logger.warning("Kill Switch 발동 상태 - 매매 중단")
                    await asyncio.sleep(60)
                    continue

                # 시간대별 전략 필터링
                active_now = self._filter_strategies_by_time(datetime.now())

                # 각 전략별 신호 생성
                for strategy in active_now:
                    for candidate in self.daily_candidates:
                        try:
                            market_data = self.data_hub.get_market_data(
                                candidate.stock_code
                            )
                            if market_data is None:
                                continue

                            signal = await strategy.generate_signal(
                                candidate, market_data
                            )
                            if signal is None:
                                continue

                            # 리스크 검증
                            positions = self.order_manager.get_open_positions()
                            passed, details = await self.risk_manager.validate_signal(
                                signal=signal,
                                current_positions=positions,
                                regime=self.current_regime,
                                total_capital=self.settings.TOTAL_CAPITAL,
                            )

                            if passed:
                                # 주문 실행
                                result = await self.order_manager.place_order(signal)
                                logger.info(
                                    "주문 실행",
                                    stock=signal.stock_code,
                                    strategy=signal.strategy_code,
                                    action=signal.action,
                                    result=result,
                                )
                            else:
                                logger.info(
                                    "리스크 검증 실패",
                                    stock=signal.stock_code,
                                    strategy=signal.strategy_code,
                                    reason=details.get("reason", "unknown"),
                                )

                        except Exception as e:
                            logger.error(
                                "전략 신호 처리 오류",
                                strategy=strategy.strategy_code,
                                stock=candidate.stock_code,
                                error=str(e),
                            )

                # 보유 포지션 트레일링 스탑 확인
                await self._check_trailing_stops()

                # 1초 대기 (과도한 루프 방지)
                await asyncio.sleep(1)

            except Exception as e:
                logger.error("매매 루프 오류", error=str(e))
                await asyncio.sleep(5)

        logger.info("매매 루프 종료")

    def _filter_strategies_by_time(self, now: datetime) -> list:
        """시간대별 전략 필터링 (부록 B 기준)"""
        hour, minute = now.hour, now.minute
        t = hour * 60 + minute

        if t < 9 * 60 + 15:
            # 09:00~09:15: 관찰만
            return []
        elif t < 9 * 60 + 30:
            # 09:15~09:30: Gap & Go만
            return [s for s in self.active_strategies if s.strategy_code == "S2"]
        elif t < 10 * 60 + 30:
            # 09:30~10:30: 승률 최고 구간, 전 전략 활성화
            return self.active_strategies
        elif t < 14 * 60:
            # 10:30~14:00: 횡보, GR/B3만
            return [
                s for s in self.active_strategies
                if s.strategy_code in ("GR", "B3")
            ]
        elif t < 15 * 60 + 20:
            # 14:00~15:20: 청산 위주
            return []  # 청산은 trailing stop에서 처리
        else:
            # 15:20~15:30: 종가 단일가 (DS 활성화)
            return [s for s in self.active_strategies if s.strategy_code == "DS"]

    async def _check_trailing_stops(self):
        """보유 포지션 트레일링 스탑 확인"""
        positions = self.order_manager.get_open_positions()
        for position in positions:
            stock_code = position.get("stock_code")
            price_data = self.cache.get_price(stock_code)
            if price_data is None:
                continue

            trailing = position.get("trailing_stop")
            if trailing is None:
                continue

            triggered, reason = trailing.update_and_check(
                current_price=price_data.price,
                market_data=position.get("market_data", {}),
            )
            if triggered:
                logger.info(
                    "트레일링 스탑 발동",
                    stock=stock_code,
                    reason=reason,
                )
                # 매도 주문 생성
                sell_signal = TradeSignal(
                    stock_code=stock_code,
                    action="SELL",
                    strategy_code=position.get("strategy_code", ""),
                    entry_price=position.get("entry_price", 0),
                    stop_loss=0,
                    target_prices=[],
                    position_pct=position.get("position_pct", 0),
                    confidence=0,
                    reason=f"트레일링 스탑: {reason}",
                    indicators_snapshot={},
                )
                await self.order_manager.place_order(sell_signal)

    async def market_close(self):
        """15:30 장 마감 처리"""
        logger.info("=== 장 마감 처리 시작 ===")
        try:
            # 1. 미체결 주문 전량 취소
            cancelled = await self.order_manager.cancel_all_pending()
            logger.info("미체결 주문 취소", count=len(cancelled))

            # 2. 변동성 돌파 전략 당일 포지션 청산
            positions = self.order_manager.get_open_positions()
            for pos in positions:
                if pos.get("strategy_code") == "VB":
                    sell_signal = TradeSignal(
                        stock_code=pos["stock_code"],
                        action="SELL",
                        strategy_code="VB",
                        entry_price=pos.get("entry_price", 0),
                        stop_loss=0,
                        target_prices=[],
                        position_pct=pos.get("position_pct", 0),
                        confidence=0,
                        reason="변동성 돌파 전략 장 마감 청산",
                        indicators_snapshot={},
                    )
                    await self.order_manager.place_order(sell_signal)

            # 3. OrderTracker 중지
            self.order_manager.order_tracker.stop_tracking()

            logger.info("=== 장 마감 처리 완료 ===")

        except Exception as e:
            logger.error("장 마감 처리 실패", error=str(e))
            await self.notifier.send_critical(f"장 마감 처리 실패: {e}")

    async def flush_redis_to_db(self):
        """15:40 Redis → DB Bulk Insert"""
        logger.info("=== Redis → DB Flush 시작 ===")
        try:
            today = datetime.now().strftime("%Y%m%d")
            async with self.repository.get_session() as session:
                count = await self.redis_buffer.flush_to_db(today, session)
            logger.info("Redis → DB Flush 완료", rows=count)
        except Exception as e:
            logger.error("Redis Flush 실패", error=str(e))

    async def generate_daily_report(self):
        """16:00 일간 성과 리포트"""
        logger.info("=== 일간 리포트 생성 ===")
        try:
            today = date.today()
            review = await self.reviewer.generate_daily_review(today)
            report_text = self.reviewer.format_review_text(review)

            await self.notifier.send_daily_report(report_text)
            logger.info("일간 리포트 전송 완료")

            # 드로우다운 체크
            daily_pnl_pct = review.get("daily_pnl_pct", 0)
            monthly_pnl_pct = review.get("monthly_pnl_pct", 0)
            cumulative_pnl_pct = review.get("cumulative_pnl_pct", 0)

            dd_result = await self.risk_manager.drawdown_protocol.evaluate_and_respond(
                daily_pnl_pct=daily_pnl_pct,
                monthly_pnl_pct=monthly_pnl_pct,
                cumulative_pnl_pct=cumulative_pnl_pct,
            )
            if dd_result.get("level") != "GREEN":
                logger.warning("드로우다운 경고", level=dd_result["level"])

        except Exception as e:
            logger.error("일간 리포트 생성 실패", error=str(e))

    async def cleanup(self):
        """16:30 일일 정리"""
        logger.info("=== 일일 정리 시작 ===")
        try:
            # WebSocket 연결 종료
            if self.ws_client:
                await self.ws_client.disconnect()

            # Redis 캐시 초기화
            await self.redis_buffer.clear_day_cache()

            logger.info("=== 일일 정리 완료 ===")
        except Exception as e:
            logger.error("일일 정리 실패", error=str(e))

    async def generate_weekly_review(self):
        """매주 금요일 주간 리뷰"""
        logger.info("=== 주간 리뷰 생성 ===")
        try:
            week_start = date.today() - timedelta(days=date.today().weekday())
            review = await self.reviewer.generate_weekly_review(week_start)
            report_text = self.reviewer.format_review_text(review)
            await self.notifier.send_daily_report(report_text)
            logger.info("주간 리뷰 전송 완료")
        except Exception as e:
            logger.error("주간 리뷰 생성 실패", error=str(e))

    async def generate_monthly_review(self):
        """매월 말 월간 리뷰"""
        logger.info("=== 월간 리뷰 생성 ===")
        try:
            today = date.today()
            review = await self.reviewer.generate_monthly_review(today.year, today.month)
            report_text = self.reviewer.format_review_text(review)
            await self.notifier.send_daily_report(report_text)
            logger.info("월간 리뷰 전송 완료")
        except Exception as e:
            logger.error("월간 리뷰 생성 실패", error=str(e))

    # ===== 시스템 제어 =====

    async def start(self):
        """시스템 시작"""
        logger.info(
            "=" * 60 + "\n"
            "  KATS (KIS Auto Trading System) v1.1 시작\n"
            "=" * 60,
            mode=self.settings.TRADE_MODE,
        )

        self._running = True

        # 컴포넌트 초기화
        await self.init_components()

        # 스케줄러 설정 및 시작
        self.setup_scheduler()
        self.scheduler.start()

        # 시스템 시작 알림
        await self.notifier.send_trade_notification(
            trade=None,
            r_multiple=0,
            custom_message=(
                f"KATS v1.1 시스템 시작\n"
                f"모드: {self.settings.TRADE_MODE}\n"
                f"총 자본: {self.settings.TOTAL_CAPITAL:,}원"
            ),
        )

        # 현재 시각에 따라 즉시 실행할 작업 결정
        now = datetime.now()
        if now.hour >= 6 and now.hour < 9:
            await self.health_check()
            if now.hour >= 8:
                await self.refresh_token()
            if now.hour >= 8 and now.minute >= 30:
                await self.scan_stocks()
        elif now.hour == 9 and now.minute < 30:
            # 장 시작 직전/직후
            await self.refresh_token()
            await self.scan_stocks()
            await self.prepare_strategies()
            if now.minute >= 0:
                await self.start_trading()

        # 이벤트 루프 유지
        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("시스템 종료 요청 수신")

    async def stop(self):
        """시스템 정지"""
        logger.info("시스템 종료 시작")
        self._running = False

        # 미체결 주문 취소
        if self.order_manager:
            await self.order_manager.cancel_all_pending()

        # WebSocket 종료
        if self.ws_client:
            await self.ws_client.disconnect()

        # REST client 종료
        if self.rest_client:
            await self.rest_client.close()

        # 스케줄러 종료
        if self.scheduler.running:
            self.scheduler.shutdown()

        logger.info("시스템 종료 완료")


async def main():
    """메인 함수"""
    setup_logging()

    system = KATSSystem()

    # 시그널 핸들러 설정
    loop = asyncio.get_running_loop()

    def signal_handler():
        logger.info("종료 시그널 수신")
        asyncio.create_task(system.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await system.start()
    except KeyboardInterrupt:
        logger.info("키보드 인터럽트")
    finally:
        await system.stop()


if __name__ == "__main__":
    asyncio.run(main())
