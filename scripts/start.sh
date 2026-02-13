#!/usr/bin/env bash
# =============================================================================
# KATS (KIS Auto Trading System) — 시스템 기동 스크립트
# 사용법: ./scripts/start.sh [--live] [--skip-redis]
# =============================================================================
set -euo pipefail

# ── 경로 설정 ──────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_DIR="$PROJECT_DIR/.pids"
LOG_DIR="$PROJECT_DIR/logs"
ENV_FILE="$PROJECT_DIR/.env"
VENV_DIR="$PROJECT_DIR/.venv"

# ── 색상 ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# ── 유틸 함수 ──────────────────────────────────────────────────────────────
log_info()  { echo -e "${GREEN}[INFO]${NC}  $(date '+%H:%M:%S') $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $(date '+%H:%M:%S') $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%H:%M:%S') $*"; }
log_step()  { echo -e "${CYAN}[STEP]${NC}  $(date '+%H:%M:%S') ${BOLD}$*${NC}"; }

separator() {
    echo -e "${BLUE}──────────────────────────────────────────────────────────${NC}"
}

# ── 옵션 파싱 ──────────────────────────────────────────────────────────────
TRADE_MODE="PAPER"
SKIP_REDIS=false

for arg in "$@"; do
    case $arg in
        --live)       TRADE_MODE="LIVE" ;;
        --skip-redis) SKIP_REDIS=true ;;
        --help|-h)
            echo "사용법: $0 [옵션]"
            echo ""
            echo "옵션:"
            echo "  --live         실전 매매 모드로 시작 (기본: PAPER)"
            echo "  --skip-redis   Redis 시작 건너뛰기 (이미 실행 중인 경우)"
            echo "  -h, --help     도움말 표시"
            exit 0
            ;;
        *)
            log_error "알 수 없는 옵션: $arg"
            exit 1
            ;;
    esac
done

# ── 시작 ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${CYAN}"
echo "  ╔═══════════════════════════════════════════════════════╗"
echo "  ║           KATS v1.1 — 시스템 기동 스크립트           ║"
echo "  ║         KIS Auto Trading System Launcher             ║"
echo "  ╚═══════════════════════════════════════════════════════╝"
echo -e "${NC}"
separator

# ── 0단계: 디렉토리 준비 ──────────────────────────────────────────────────
mkdir -p "$PID_DIR" "$LOG_DIR"

# ── 1단계: 환경 파일 확인 ──────────────────────────────────────────────────
log_step "1/6 환경 설정 확인"

if [ ! -f "$ENV_FILE" ]; then
    log_warn ".env 파일이 없습니다. .env.example에서 복사합니다."
    if [ -f "$PROJECT_DIR/.env.example" ]; then
        cp "$PROJECT_DIR/.env.example" "$ENV_FILE"
        log_warn ".env 파일이 생성되었습니다. API 키를 설정한 후 다시 실행하세요."
        log_error "  -> $ENV_FILE 편집 필요"
        exit 1
    else
        log_error ".env.example 파일도 없습니다. 프로젝트 구조를 확인하세요."
        exit 1
    fi
fi

# .env에서 필수 키 확인
source "$ENV_FILE" 2>/dev/null || true
if [ -z "${KIS_APP_KEY:-}" ] || [ "$KIS_APP_KEY" = "your_app_key_here" ]; then
    log_error "KIS_APP_KEY가 설정되지 않았습니다."
    log_error "  -> $ENV_FILE 에서 API 키를 설정하세요."
    exit 1
fi

# 커맨드라인 --live 옵션으로 모드 오버라이드
export TRADE_MODE="$TRADE_MODE"

if [ "$TRADE_MODE" = "LIVE" ]; then
    echo ""
    log_warn "================================================"
    log_warn "  실전 매매(LIVE) 모드로 시작합니다!"
    log_warn "  실제 자금이 사용됩니다. 주의하세요."
    log_warn "================================================"
    echo ""
    read -r -p "계속하시겠습니까? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        log_info "사용자가 취소했습니다."
        exit 0
    fi
fi

log_info "매매 모드: ${BOLD}$TRADE_MODE${NC}"
separator

# ── 2단계: Python 가상환경 확인 ────────────────────────────────────────────
log_step "2/6 Python 가상환경 확인"

if [ -d "$VENV_DIR" ]; then
    source "$VENV_DIR/bin/activate"
    log_info "가상환경 활성화: $VENV_DIR"
else
    log_warn "가상환경이 없습니다. 시스템 Python을 사용합니다."
    log_warn "  가상환경 생성: python3 -m venv $VENV_DIR"
fi

# Python 버전 확인
PYTHON_VERSION=$(python3 --version 2>&1)
log_info "Python: $PYTHON_VERSION"

# 의존성 확인 (빠른 체크)
if ! python3 -c "import aiohttp, websockets, sqlalchemy, redis, apscheduler, structlog" 2>/dev/null; then
    log_warn "필수 패키지가 설치되지 않았습니다. 설치를 시작합니다..."
    pip install -r "$PROJECT_DIR/requirements.txt" --quiet
    log_info "패키지 설치 완료"
else
    log_info "필수 패키지 확인 완료"
fi
separator

# ── 3단계: Redis 서버 시작 ─────────────────────────────────────────────────
log_step "3/6 Redis 서버 확인 및 시작"

REDIS_PID_FILE="$PID_DIR/redis.pid"

start_redis() {
    # Redis가 이미 실행 중인지 확인
    if redis-cli ping >/dev/null 2>&1; then
        REDIS_PID=$(pgrep -f "redis-server" | head -1 || echo "unknown")
        log_info "Redis 이미 실행 중 (PID: $REDIS_PID)"
        return 0
    fi

    log_info "Redis 서버 시작..."

    # Redis 설정 파일 생성 (없으면)
    REDIS_CONF="$PROJECT_DIR/redis.conf"
    if [ ! -f "$REDIS_CONF" ]; then
        cat > "$REDIS_CONF" << 'REDIS_EOF'
# KATS Redis Configuration
bind 127.0.0.1
port 6379
daemonize yes
pidfile ./pids/redis.pid
logfile ./logs/redis.log

# 메모리 설정 (실시간 틱 버퍼용)
maxmemory 512mb
maxmemory-policy allkeys-lru

# 데이터 지속성 (RDB 스냅샷)
save 900 1
save 300 10
save 60 10000
dbfilename kats_redis.rdb
dir ./data

# 성능 최적화
tcp-keepalive 60
timeout 0
REDIS_EOF
        mkdir -p "$PROJECT_DIR/data"
        log_info "Redis 설정 파일 생성: $REDIS_CONF"
    fi

    # PID 파일 경로를 절대 경로로 변환
    sed -i "s|pidfile .*|pidfile $REDIS_PID_FILE|" "$REDIS_CONF"
    sed -i "s|logfile .*|logfile $LOG_DIR/redis.log|" "$REDIS_CONF"
    sed -i "s|dir .*|dir $PROJECT_DIR/data|" "$REDIS_CONF"

    redis-server "$REDIS_CONF"

    # 시작 대기 (최대 5초)
    for i in $(seq 1 10); do
        if redis-cli ping >/dev/null 2>&1; then
            REDIS_PID=$(cat "$REDIS_PID_FILE" 2>/dev/null || pgrep -f "redis-server" | head -1)
            log_info "Redis 시작 완료 (PID: $REDIS_PID)"
            return 0
        fi
        sleep 0.5
    done

    log_error "Redis 시작 실패. 로그를 확인하세요: $LOG_DIR/redis.log"
    exit 1
}

if [ "$SKIP_REDIS" = true ]; then
    if redis-cli ping >/dev/null 2>&1; then
        log_info "Redis 건너뛰기 (--skip-redis, 이미 실행 중)"
    else
        log_error "Redis가 실행 중이 아닙니다. --skip-redis 옵션을 제거하세요."
        exit 1
    fi
else
    start_redis
fi

# Redis 연결 테스트
REDIS_INFO=$(redis-cli info server 2>/dev/null | grep redis_version | cut -d: -f2 | tr -d '\r')
log_info "Redis 버전: $REDIS_INFO"
separator

# ── 4단계: 데이터베이스 초기화 ─────────────────────────────────────────────
log_step "4/6 데이터베이스 초기화"

python3 -c "
import asyncio
import sys
sys.path.insert(0, '$PROJECT_DIR')
from kats.database.repository import Repository
from kats.config.settings import Settings

async def init():
    repo = Repository(Settings.DB_URL)
    await repo.init_db()
    print('DB 초기화 완료')

asyncio.run(init())
" 2>&1 | while read -r line; do log_info "$line"; done

separator

# ── 5단계: 사전 검증 (프리플라이트 체크) ───────────────────────────────────
log_step "5/6 사전 검증 (프리플라이트 체크)"

python3 << 'PREFLIGHT_EOF'
import asyncio
import sys
import os

sys.path.insert(0, os.environ.get("PROJECT_DIR", "."))

async def preflight():
    checks = []

    # 1. 설정 로드
    try:
        from kats.config.settings import Settings
        checks.append(("환경 설정 로드", True, f"모드={Settings.TRADE_MODE}"))
    except Exception as e:
        checks.append(("환경 설정 로드", False, str(e)))

    # 2. Redis 연결
    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(Settings.REDIS_URL)
        await r.ping()
        await r.aclose()
        checks.append(("Redis 연결", True, Settings.REDIS_URL))
    except Exception as e:
        checks.append(("Redis 연결", False, str(e)))

    # 3. DB 연결
    try:
        from sqlalchemy.ext.asyncio import create_async_engine
        engine = create_async_engine(Settings.DB_URL)
        async with engine.begin() as conn:
            pass
        await engine.dispose()
        checks.append(("DB 연결", True, Settings.DB_URL))
    except Exception as e:
        checks.append(("DB 연결", False, str(e)))

    # 4. API 키 존재
    has_key = bool(Settings.KIS_APP_KEY and Settings.KIS_APP_KEY != "your_app_key_here")
    checks.append(("KIS API 키", has_key, "설정됨" if has_key else "미설정"))

    # 5. 모듈 임포트
    try:
        from kats.strategy.strategy_selector import StrategySelector
        from kats.risk.risk_manager import RiskManager
        from kats.order.order_manager import OrderManager
        checks.append(("핵심 모듈 임포트", True, "strategy/risk/order OK"))
    except Exception as e:
        checks.append(("핵심 모듈 임포트", False, str(e)))

    # 결과 출력
    all_passed = True
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        symbol = "✓" if passed else "✗"
        print(f"  {symbol} {name}: {status} ({detail})")
        if not passed:
            all_passed = False

    if not all_passed:
        print("\n  일부 검증 실패. 위 항목을 확인하세요.")
        sys.exit(1)
    else:
        print("\n  모든 검증 통과!")

asyncio.run(preflight())
PREFLIGHT_EOF

PREFLIGHT_EXIT=$?
if [ $PREFLIGHT_EXIT -ne 0 ]; then
    log_error "사전 검증 실패. 시스템을 시작할 수 없습니다."
    exit 1
fi
separator

# ── 6단계: KATS 시스템 시작 ────────────────────────────────────────────────
log_step "6/6 KATS 시스템 시작"

KATS_PID_FILE="$PID_DIR/kats.pid"
KATS_LOG="$LOG_DIR/kats_$(date '+%Y%m%d_%H%M%S').log"

# 이미 실행 중인지 확인
if [ -f "$KATS_PID_FILE" ]; then
    OLD_PID=$(cat "$KATS_PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        log_warn "KATS가 이미 실행 중입니다 (PID: $OLD_PID)"
        log_warn "중지 후 재시작하려면: ./scripts/restart.sh"
        exit 1
    else
        log_warn "이전 PID 파일 정리 (PID $OLD_PID 이미 종료됨)"
        rm -f "$KATS_PID_FILE"
    fi
fi

# 백그라운드로 KATS 시작
cd "$PROJECT_DIR"
export TRADE_MODE
nohup python3 -m kats.main > "$KATS_LOG" 2>&1 &
KATS_PID=$!
echo "$KATS_PID" > "$KATS_PID_FILE"

# 시작 확인 (최대 5초)
sleep 2
if kill -0 "$KATS_PID" 2>/dev/null; then
    log_info "KATS 시스템 시작 완료!"
else
    log_error "KATS 시스템 시작 실패. 로그를 확인하세요:"
    log_error "  tail -50 $KATS_LOG"
    exit 1
fi

separator
echo ""
echo -e "${GREEN}${BOLD}  KATS v1.1 시스템이 성공적으로 시작되었습니다!${NC}"
echo ""
echo -e "  매매 모드:  ${BOLD}$TRADE_MODE${NC}"
echo -e "  KATS PID:   ${BOLD}$KATS_PID${NC}"
echo -e "  로그 파일:  ${BOLD}$KATS_LOG${NC}"
echo -e "  PID 파일:   ${BOLD}$KATS_PID_FILE${NC}"
echo ""
echo -e "  ${CYAN}실시간 로그 보기:${NC}"
echo -e "    tail -f $KATS_LOG"
echo ""
echo -e "  ${CYAN}시스템 상태 확인:${NC}"
echo -e "    ./scripts/status.sh"
echo ""
echo -e "  ${CYAN}시스템 중지:${NC}"
echo -e "    ./scripts/stop.sh"
echo ""
separator
