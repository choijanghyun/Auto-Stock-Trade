#!/usr/bin/env bash
# =============================================================================
# KATS (KIS Auto Trading System) — 시스템 재시작 스크립트
# 사용법: ./scripts/restart.sh [--live] [--force]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo ""
echo -e "\033[1;36m"
echo "  ╔═══════════════════════════════════════════════════════╗"
echo "  ║          KATS v1.1 — 시스템 재시작 스크립트          ║"
echo "  ╚═══════════════════════════════════════════════════════╝"
echo -e "\033[0m"

# 옵션 전달
STOP_ARGS=""
START_ARGS=""
for arg in "$@"; do
    case $arg in
        --force)  STOP_ARGS="$STOP_ARGS --force" ;;
        --live)   START_ARGS="$START_ARGS --live" ;;
        --help|-h)
            echo "사용법: $0 [옵션]"
            echo ""
            echo "옵션:"
            echo "  --live    실전 매매 모드로 재시작"
            echo "  --force   강제 종료 후 재시작"
            echo "  -h, --help 도움말 표시"
            exit 0
            ;;
    esac
done

echo -e "\033[1;33m[1/2] 시스템 중지...\033[0m"
echo ""
bash "$SCRIPT_DIR/stop.sh" $STOP_ARGS

echo ""
echo -e "\033[1;33m[2/2] 시스템 시작...\033[0m"
echo ""

# Redis는 이미 실행 중이므로 건너뛰기
bash "$SCRIPT_DIR/start.sh" --skip-redis $START_ARGS
