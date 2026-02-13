#!/usr/bin/env bash
# =============================================================================
# KATS (KIS Auto Trading System) — 시스템 중지 스크립트
# 사용법: ./scripts/stop.sh [--all] [--force]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_DIR="$PROJECT_DIR/.pids"
LOG_DIR="$PROJECT_DIR/logs"

# ── 색상 ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $(date '+%H:%M:%S') $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $(date '+%H:%M:%S') $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%H:%M:%S') $*"; }
log_step()  { echo -e "${CYAN}[STEP]${NC}  $(date '+%H:%M:%S') ${BOLD}$*${NC}"; }

separator() {
    echo -e "${BLUE}──────────────────────────────────────────────────────────${NC}"
}

# ── 옵션 파싱 ──────────────────────────────────────────────────────────────
STOP_REDIS=false
FORCE_KILL=false

for arg in "$@"; do
    case $arg in
        --all)   STOP_REDIS=true ;;
        --force) FORCE_KILL=true ;;
        --help|-h)
            echo "사용법: $0 [옵션]"
            echo ""
            echo "옵션:"
            echo "  --all     Redis 서버도 함께 중지"
            echo "  --force   강제 종료 (SIGKILL 사용)"
            echo "  -h, --help 도움말 표시"
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
echo -e "${BOLD}${RED}"
echo "  ╔═══════════════════════════════════════════════════════╗"
echo "  ║           KATS v1.1 — 시스템 중지 스크립트           ║"
echo "  ╚═══════════════════════════════════════════════════════╝"
echo -e "${NC}"
separator

# ── 프로세스 종료 함수 ─────────────────────────────────────────────────────
stop_process() {
    local name="$1"
    local pid_file="$2"
    local grace_sec="${3:-10}"

    if [ ! -f "$pid_file" ]; then
        log_warn "$name: PID 파일 없음 ($pid_file)"
        return 0
    fi

    local pid
    pid=$(cat "$pid_file")

    if ! kill -0 "$pid" 2>/dev/null; then
        log_warn "$name: 프로세스 이미 종료됨 (PID: $pid)"
        rm -f "$pid_file"
        return 0
    fi

    log_info "$name 종료 요청 (PID: $pid)..."

    if [ "$FORCE_KILL" = true ]; then
        kill -9 "$pid" 2>/dev/null || true
        log_warn "$name 강제 종료 (SIGKILL)"
    else
        # Graceful shutdown: SIGTERM 후 대기
        kill -TERM "$pid" 2>/dev/null || true

        local waited=0
        while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$grace_sec" ]; do
            sleep 1
            waited=$((waited + 1))
            echo -ne "\r  대기 중... ${waited}/${grace_sec}초"
        done
        echo ""

        if kill -0 "$pid" 2>/dev/null; then
            log_warn "$name: ${grace_sec}초 내 종료되지 않음. 강제 종료합니다."
            kill -9 "$pid" 2>/dev/null || true
            sleep 1
        fi
    fi

    if ! kill -0 "$pid" 2>/dev/null; then
        log_info "$name 종료 완료 (PID: $pid)"
        rm -f "$pid_file"
        return 0
    else
        log_error "$name 종료 실패 (PID: $pid)"
        return 1
    fi
}

# ── 1단계: KATS 메인 프로세스 중지 ─────────────────────────────────────────
log_step "1/3 KATS 메인 프로세스 중지"

KATS_PID_FILE="$PID_DIR/kats.pid"

if [ -f "$KATS_PID_FILE" ]; then
    stop_process "KATS" "$KATS_PID_FILE" 15
else
    # PID 파일 없으면 프로세스명으로 검색
    KATS_PIDS=$(pgrep -f "python.*kats.main" 2>/dev/null || true)
    if [ -n "$KATS_PIDS" ]; then
        log_warn "PID 파일 없이 실행 중인 KATS 발견: $KATS_PIDS"
        for pid in $KATS_PIDS; do
            log_info "프로세스 종료: PID $pid"
            if [ "$FORCE_KILL" = true ]; then
                kill -9 "$pid" 2>/dev/null || true
            else
                kill -TERM "$pid" 2>/dev/null || true
            fi
        done
        sleep 2
    else
        log_info "실행 중인 KATS 프로세스 없음"
    fi
fi

separator

# ── 2단계: 잔여 자식 프로세스 정리 ─────────────────────────────────────────
log_step "2/3 잔여 프로세스 정리"

ORPHAN_PIDS=$(pgrep -f "python.*kats" 2>/dev/null || true)
if [ -n "$ORPHAN_PIDS" ]; then
    log_warn "잔여 KATS 프로세스 발견:"
    for pid in $ORPHAN_PIDS; do
        CMD=$(ps -p "$pid" -o args= 2>/dev/null || echo "unknown")
        log_warn "  PID $pid: $CMD"
        kill -TERM "$pid" 2>/dev/null || true
    done
    sleep 2

    # 남아있는 프로세스 강제 종료
    STILL_RUNNING=$(pgrep -f "python.*kats" 2>/dev/null || true)
    if [ -n "$STILL_RUNNING" ]; then
        log_warn "남아있는 프로세스 강제 종료"
        for pid in $STILL_RUNNING; do
            kill -9 "$pid" 2>/dev/null || true
        done
    fi
else
    log_info "잔여 프로세스 없음"
fi

separator

# ── 3단계: Redis 중지 (--all 옵션) ────────────────────────────────────────
log_step "3/3 Redis 서버 처리"

REDIS_PID_FILE="$PID_DIR/redis.pid"

if [ "$STOP_REDIS" = true ]; then
    if redis-cli ping >/dev/null 2>&1; then
        log_info "Redis 서버 종료 중..."

        # BGSAVE 후 종료 (데이터 보존)
        redis-cli bgsave >/dev/null 2>&1 || true
        sleep 1
        redis-cli shutdown nosave >/dev/null 2>&1 || true

        sleep 2
        if ! redis-cli ping >/dev/null 2>&1; then
            log_info "Redis 서버 종료 완료"
            rm -f "$REDIS_PID_FILE"
        else
            # PID 파일로 직접 종료
            if [ -f "$REDIS_PID_FILE" ]; then
                stop_process "Redis" "$REDIS_PID_FILE" 10
            else
                log_error "Redis 종료 실패. 수동으로 종료하세요: redis-cli shutdown"
            fi
        fi
    else
        log_info "Redis 이미 종료됨"
    fi
else
    if redis-cli ping >/dev/null 2>&1; then
        log_info "Redis 계속 실행 중 (종료하려면 --all 옵션 사용)"
    else
        log_info "Redis 실행 중이 아님"
    fi
fi

# ── PID 디렉토리 정리 ─────────────────────────────────────────────────────
rm -f "$PID_DIR/kats.pid"

separator
echo ""
echo -e "${GREEN}${BOLD}  KATS v1.1 시스템이 중지되었습니다.${NC}"
echo ""
if [ "$STOP_REDIS" = true ]; then
    echo -e "  Redis:  ${RED}중지${NC}"
else
    echo -e "  Redis:  ${GREEN}실행 중${NC} (--all 옵션으로 함께 중지 가능)"
fi
echo ""
echo -e "  ${CYAN}다시 시작:${NC}  ./scripts/start.sh"
echo -e "  ${CYAN}상태 확인:${NC}  ./scripts/status.sh"
echo ""
separator
