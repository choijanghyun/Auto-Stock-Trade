#!/usr/bin/env bash
# =============================================================================
# KATS (KIS Auto Trading System) — 시스템 상태 확인 스크립트
# 사용법: ./scripts/status.sh [--json]
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
DIM='\033[2m'

# ── JSON 모드 ──────────────────────────────────────────────────────────────
JSON_MODE=false
for arg in "$@"; do
    case $arg in
        --json) JSON_MODE=true ;;
        --help|-h)
            echo "사용법: $0 [옵션]"
            echo ""
            echo "옵션:"
            echo "  --json   JSON 형식으로 출력"
            echo "  -h       도움말 표시"
            exit 0
            ;;
    esac
done

# ── 상태 수집 함수 ─────────────────────────────────────────────────────────
check_process() {
    local name="$1"
    local pid_file="$2"

    if [ -f "$pid_file" ]; then
        local pid
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            local uptime_info
            uptime_info=$(ps -p "$pid" -o etime= 2>/dev/null | xargs || echo "unknown")
            local mem_info
            mem_info=$(ps -p "$pid" -o rss= 2>/dev/null | xargs || echo "0")
            mem_info=$((mem_info / 1024))  # KB -> MB
            local cpu_info
            cpu_info=$(ps -p "$pid" -o %cpu= 2>/dev/null | xargs || echo "0.0")
            echo "RUNNING|$pid|$uptime_info|${mem_info}MB|${cpu_info}%"
        else
            echo "DEAD|$pid|PID파일 존재하나 프로세스 없음"
        fi
    else
        # PID 파일 없으면 프로세스명으로 검색
        local pids
        pids=$(pgrep -f "$3" 2>/dev/null | head -1 || true)
        if [ -n "$pids" ]; then
            echo "RUNNING_NOPID|$pids|PID파일 없이 실행 중"
        else
            echo "STOPPED||"
        fi
    fi
}

check_redis() {
    if redis-cli ping >/dev/null 2>&1; then
        local redis_info
        redis_info=$(redis-cli info server 2>/dev/null)
        local version
        version=$(echo "$redis_info" | grep redis_version: | cut -d: -f2 | tr -d '\r')
        local uptime
        uptime=$(echo "$redis_info" | grep uptime_in_seconds: | cut -d: -f2 | tr -d '\r')
        local memory
        memory=$(redis-cli info memory 2>/dev/null | grep used_memory_human: | cut -d: -f2 | tr -d '\r')
        local keys
        keys=$(redis-cli dbsize 2>/dev/null | grep -o '[0-9]*' || echo "0")

        # 업타임 포맷팅
        local days=$((uptime / 86400))
        local hours=$(( (uptime % 86400) / 3600))
        local mins=$(( (uptime % 3600) / 60))
        local uptime_str="${days}d ${hours}h ${mins}m"

        echo "RUNNING|$version|$uptime_str|$memory|${keys}keys"
    else
        echo "STOPPED||||"
    fi
}

check_db() {
    local db_file="$PROJECT_DIR/kats.db"
    if [ -f "$db_file" ]; then
        local size
        size=$(du -h "$db_file" 2>/dev/null | cut -f1)
        echo "OK|$size"
    else
        echo "NOT_FOUND|"
    fi
}

get_trade_mode() {
    source "$PROJECT_DIR/.env" 2>/dev/null || true
    echo "${TRADE_MODE:-PAPER}"
}

get_latest_log() {
    local latest
    latest=$(ls -t "$LOG_DIR"/kats_*.log 2>/dev/null | head -1 || echo "")
    if [ -n "$latest" ]; then
        local size
        size=$(du -h "$latest" 2>/dev/null | cut -f1)
        local lines
        lines=$(wc -l < "$latest" 2>/dev/null || echo "0")
        local last_entry
        last_entry=$(tail -1 "$latest" 2>/dev/null | cut -c1-80 || echo "")
        echo "$latest|$size|$lines|$last_entry"
    else
        echo "없음|||"
    fi
}

# ── 상태 수집 ──────────────────────────────────────────────────────────────
KATS_STATUS=$(check_process "KATS" "$PID_DIR/kats.pid" "python.*kats.main")
REDIS_STATUS=$(check_redis)
DB_STATUS=$(check_db)
TRADE_MODE=$(get_trade_mode)
LOG_INFO=$(get_latest_log)

# ── JSON 출력 ──────────────────────────────────────────────────────────────
if [ "$JSON_MODE" = true ]; then
    IFS='|' read -r kats_state kats_pid kats_uptime kats_mem kats_cpu <<< "$KATS_STATUS"
    IFS='|' read -r redis_state redis_ver redis_uptime redis_mem redis_keys <<< "$REDIS_STATUS"
    IFS='|' read -r db_state db_size <<< "$DB_STATUS"

    cat << EOF
{
  "timestamp": "$(date -Iseconds)",
  "trade_mode": "$TRADE_MODE",
  "kats": {
    "status": "$kats_state",
    "pid": "$kats_pid",
    "uptime": "$kats_uptime",
    "memory": "$kats_mem",
    "cpu": "$kats_cpu"
  },
  "redis": {
    "status": "$redis_state",
    "version": "$redis_ver",
    "uptime": "$redis_uptime",
    "memory": "$redis_mem",
    "keys": "$redis_keys"
  },
  "database": {
    "status": "$db_state",
    "size": "$db_size"
  }
}
EOF
    exit 0
fi

# ── 콘솔 출력 ──────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${CYAN}"
echo "  ╔═══════════════════════════════════════════════════════╗"
echo "  ║       KATS v1.1 — 시스템 상태 대시보드               ║"
echo "  ╚═══════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  ${DIM}$(date '+%Y-%m-%d %H:%M:%S KST')${NC}"
echo ""

# 매매 모드 표시
if [ "$TRADE_MODE" = "LIVE" ]; then
    echo -e "  매매 모드:  ${RED}${BOLD}LIVE (실전)${NC}"
else
    echo -e "  매매 모드:  ${GREEN}${BOLD}PAPER (모의)${NC}"
fi
echo ""

echo -e "${BLUE}  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# ── KATS 프로세스 ──────────────────────────────────────────────────────────
echo -e "  ${BOLD}KATS 메인 프로세스${NC}"

IFS='|' read -r kats_state kats_pid kats_uptime kats_mem kats_cpu <<< "$KATS_STATUS"

case "$kats_state" in
    RUNNING)
        echo -e "    상태:    ${GREEN}● 실행 중${NC}"
        echo -e "    PID:     $kats_pid"
        echo -e "    가동:    $kats_uptime"
        echo -e "    메모리:  $kats_mem"
        echo -e "    CPU:     $kats_cpu"
        ;;
    RUNNING_NOPID)
        echo -e "    상태:    ${YELLOW}● 실행 중 (PID 파일 없음)${NC}"
        echo -e "    PID:     $kats_pid"
        ;;
    DEAD)
        echo -e "    상태:    ${RED}● 비정상 종료${NC}"
        echo -e "    마지막 PID: $kats_pid"
        echo -e "    ${YELLOW}→ PID 파일 존재하나 프로세스 없음. start.sh로 재시작하세요.${NC}"
        ;;
    STOPPED)
        echo -e "    상태:    ${DIM}○ 중지됨${NC}"
        ;;
esac

echo ""
echo -e "${BLUE}  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# ── Redis ──────────────────────────────────────────────────────────────────
echo -e "  ${BOLD}Redis 서버${NC}"

IFS='|' read -r redis_state redis_ver redis_uptime redis_mem redis_keys <<< "$REDIS_STATUS"

case "$redis_state" in
    RUNNING)
        echo -e "    상태:    ${GREEN}● 실행 중${NC}"
        echo -e "    버전:    $redis_ver"
        echo -e "    가동:    $redis_uptime"
        echo -e "    메모리:  $redis_mem"
        echo -e "    키 수:   $redis_keys"
        ;;
    STOPPED)
        echo -e "    상태:    ${RED}● 중지됨${NC}"
        echo -e "    ${YELLOW}→ start.sh로 Redis를 시작하세요.${NC}"
        ;;
esac

echo ""
echo -e "${BLUE}  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# ── 데이터베이스 ───────────────────────────────────────────────────────────
echo -e "  ${BOLD}데이터베이스 (SQLite)${NC}"

IFS='|' read -r db_state db_size <<< "$DB_STATUS"

case "$db_state" in
    OK)
        echo -e "    상태:    ${GREEN}● 정상${NC}"
        echo -e "    크기:    $db_size"
        echo -e "    경로:    $PROJECT_DIR/kats.db"
        ;;
    NOT_FOUND)
        echo -e "    상태:    ${YELLOW}○ 미생성${NC}"
        echo -e "    ${DIM}→ 시스템 시작 시 자동 생성됩니다.${NC}"
        ;;
esac

echo ""
echo -e "${BLUE}  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# ── 로그 ───────────────────────────────────────────────────────────────────
echo -e "  ${BOLD}로그${NC}"

IFS='|' read -r log_file log_size log_lines log_last <<< "$LOG_INFO"

if [ "$log_file" != "없음" ]; then
    echo -e "    최신:    $(basename "$log_file")"
    echo -e "    크기:    ${log_size} (${log_lines}줄)"
    if [ -n "$log_last" ]; then
        echo -e "    마지막:  ${DIM}${log_last}${NC}"
    fi
else
    echo -e "    ${DIM}로그 파일 없음${NC}"
fi

echo ""
echo -e "${BLUE}  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# ── 종합 판단 ──────────────────────────────────────────────────────────────
echo ""
if [ "$kats_state" = "RUNNING" ] && [ "$redis_state" = "RUNNING" ]; then
    echo -e "  ${GREEN}${BOLD}시스템 정상 운영 중${NC}"
elif [ "$kats_state" = "STOPPED" ] && [ "$redis_state" = "STOPPED" ]; then
    echo -e "  ${DIM}시스템 전체 중지 상태${NC}"
elif [ "$kats_state" = "DEAD" ]; then
    echo -e "  ${RED}${BOLD}시스템 비정상! 재시작이 필요합니다.${NC}"
    echo -e "  ${YELLOW}→ ./scripts/restart.sh${NC}"
elif [ "$redis_state" = "STOPPED" ]; then
    echo -e "  ${YELLOW}${BOLD}Redis 중지됨. KATS가 정상 작동하지 않을 수 있습니다.${NC}"
    echo -e "  ${YELLOW}→ ./scripts/start.sh${NC}"
else
    echo -e "  ${YELLOW}부분 실행 상태. 위 상태를 확인하세요.${NC}"
fi

echo ""
echo -e "  ${DIM}명령어: start.sh | stop.sh | restart.sh | status.sh --json${NC}"
echo ""
