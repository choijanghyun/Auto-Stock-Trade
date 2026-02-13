#!/usr/bin/env python3
"""
KATS Control — Python 통합 관리 CLI

사용법:
    python scripts/kats_ctl.py start [--live]
    python scripts/kats_ctl.py stop [--all] [--force]
    python scripts/kats_ctl.py restart [--live]
    python scripts/kats_ctl.py status [--json]
    python scripts/kats_ctl.py logs [--tail N] [--follow]
    python scripts/kats_ctl.py health
    python scripts/kats_ctl.py db-init
    python scripts/kats_ctl.py db-stats
    python scripts/kats_ctl.py redis-flush [--date YYYYMMDD]
    python scripts/kats_ctl.py config [KEY] [VALUE]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, date
from pathlib import Path

# 프로젝트 루트를 path에 추가
PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))
os.chdir(str(PROJECT_DIR))

# ── 색상 유틸 ──────────────────────────────────────────────────────────────
class Color:
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    BLUE = "\033[0;34m"
    CYAN = "\033[0;36m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    NC = "\033[0m"

    @staticmethod
    def ok(text: str) -> str:
        return f"{Color.GREEN}{text}{Color.NC}"

    @staticmethod
    def warn(text: str) -> str:
        return f"{Color.YELLOW}{text}{Color.NC}"

    @staticmethod
    def err(text: str) -> str:
        return f"{Color.RED}{text}{Color.NC}"

    @staticmethod
    def info(text: str) -> str:
        return f"{Color.CYAN}{text}{Color.NC}"

    @staticmethod
    def bold(text: str) -> str:
        return f"{Color.BOLD}{text}{Color.NC}"


PID_DIR = PROJECT_DIR / ".pids"
LOG_DIR = PROJECT_DIR / "logs"
PID_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)


# ── 프로세스 유틸 ──────────────────────────────────────────────────────────
def get_pid(name: str) -> int | None:
    pid_file = PID_DIR / f"{name}.pid"
    if not pid_file.exists():
        return None
    pid = int(pid_file.read_text().strip())
    try:
        os.kill(pid, 0)
        return pid
    except OSError:
        pid_file.unlink(missing_ok=True)
        return None


def is_redis_running() -> bool:
    try:
        result = subprocess.run(
            ["redis-cli", "ping"],
            capture_output=True, text=True, timeout=3,
        )
        return result.stdout.strip() == "PONG"
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def redis_info() -> dict:
    if not is_redis_running():
        return {}
    try:
        result = subprocess.run(
            ["redis-cli", "info"],
            capture_output=True, text=True, timeout=3,
        )
        info = {}
        for line in result.stdout.splitlines():
            if ":" in line and not line.startswith("#"):
                k, v = line.split(":", 1)
                info[k.strip()] = v.strip()
        return info
    except Exception:
        return {}


# ══════════════════════════════════════════════════════════════════════════
#  명령어 구현
# ══════════════════════════════════════════════════════════════════════════

def cmd_start(args):
    """시스템 기동"""
    script = PROJECT_DIR / "scripts" / "start.sh"
    cmd = ["bash", str(script)]
    if args.live:
        cmd.append("--live")
    if args.skip_redis:
        cmd.append("--skip-redis")
    os.execvp("bash", cmd)


def cmd_stop(args):
    """시스템 중지"""
    script = PROJECT_DIR / "scripts" / "stop.sh"
    cmd = ["bash", str(script)]
    if args.all:
        cmd.append("--all")
    if args.force:
        cmd.append("--force")
    os.execvp("bash", cmd)


def cmd_restart(args):
    """시스템 재시작"""
    script = PROJECT_DIR / "scripts" / "restart.sh"
    cmd = ["bash", str(script)]
    if args.live:
        cmd.append("--live")
    if args.force:
        cmd.append("--force")
    os.execvp("bash", cmd)


def cmd_status(args):
    """시스템 상태 확인"""
    script = PROJECT_DIR / "scripts" / "status.sh"
    cmd = ["bash", str(script)]
    if args.json:
        cmd.append("--json")
    os.execvp("bash", cmd)


def cmd_logs(args):
    """로그 보기"""
    log_files = sorted(LOG_DIR.glob("kats_*.log"), reverse=True)

    if not log_files:
        print(Color.warn("로그 파일이 없습니다."))
        return

    log_file = log_files[0]
    print(f"{Color.info('로그 파일:')} {log_file}")
    print()

    if args.follow:
        # tail -f 모드
        try:
            subprocess.run(["tail", "-f", "-n", str(args.tail), str(log_file)])
        except KeyboardInterrupt:
            print("\n로그 모니터링 종료")
    else:
        # 마지막 N줄 출력
        try:
            result = subprocess.run(
                ["tail", "-n", str(args.tail), str(log_file)],
                capture_output=True, text=True,
            )
            print(result.stdout)
        except Exception as e:
            print(Color.err(f"로그 읽기 실패: {e}"))


def cmd_health(args):
    """헬스 체크 (상세)"""
    print(f"\n{Color.bold('KATS 헬스 체크')}")
    print("=" * 55)

    checks = []

    # 1. KATS 프로세스
    kats_pid = get_pid("kats")
    if kats_pid:
        checks.append(("KATS 프로세스", True, f"PID {kats_pid}"))
    else:
        checks.append(("KATS 프로세스", False, "미실행"))

    # 2. Redis
    if is_redis_running():
        info = redis_info()
        ver = info.get("redis_version", "?")
        mem = info.get("used_memory_human", "?")
        checks.append(("Redis 서버", True, f"v{ver}, {mem}"))
    else:
        checks.append(("Redis 서버", False, "미실행"))

    # 3. .env 파일
    env_file = PROJECT_DIR / ".env"
    if env_file.exists():
        from dotenv import dotenv_values
        env = dotenv_values(str(env_file))
        has_key = bool(env.get("KIS_APP_KEY")) and env["KIS_APP_KEY"] != "your_app_key_here"
        checks.append((".env 설정", has_key, "API 키 설정됨" if has_key else "API 키 미설정"))
    else:
        checks.append((".env 설정", False, "파일 없음"))

    # 4. DB 파일
    db_file = PROJECT_DIR / "kats.db"
    if db_file.exists():
        size_mb = db_file.stat().st_size / (1024 * 1024)
        checks.append(("SQLite DB", True, f"{size_mb:.1f}MB"))
    else:
        checks.append(("SQLite DB", None, "미생성 (시작 시 자동 생성)"))

    # 5. 로그 디렉토리
    log_files = list(LOG_DIR.glob("kats_*.log"))
    checks.append(("로그 파일", True if log_files else None, f"{len(log_files)}개"))

    # 6. 모듈 임포트
    try:
        from kats.config.settings import Settings
        from kats.strategy.strategy_selector import StrategySelector
        from kats.risk.risk_manager import RiskManager
        checks.append(("Python 모듈", True, "핵심 모듈 로드 성공"))
    except Exception as e:
        checks.append(("Python 모듈", False, str(e)[:50]))

    # 7. 디스크 공간
    try:
        import shutil
        usage = shutil.disk_usage(str(PROJECT_DIR))
        free_gb = usage.free / (1024 ** 3)
        checks.append(("디스크 여유", free_gb > 1, f"{free_gb:.1f}GB"))
    except Exception:
        checks.append(("디스크 여유", None, "확인 불가"))

    # 결과 출력
    print()
    all_ok = True
    for name, status, detail in checks:
        if status is True:
            icon = Color.ok("✓")
        elif status is False:
            icon = Color.err("✗")
            all_ok = False
        else:
            icon = Color.warn("–")

        print(f"  {icon}  {name:20s}  {detail}")

    print()
    if all_ok:
        print(f"  {Color.ok(Color.bold('모든 항목 정상'))}")
    else:
        print(f"  {Color.warn('일부 항목 확인 필요')}")
    print()


def cmd_db_init(args):
    """데이터베이스 초기화"""
    print(f"{Color.info('데이터베이스 초기화 중...')}")

    async def _init():
        from kats.database.repository import Repository
        from kats.config.settings import Settings
        repo = Repository(Settings.DB_URL)
        await repo.init_db()
        print(f"{Color.ok('DB 초기화 완료')}: {Settings.DB_URL}")

    asyncio.run(_init())


def cmd_db_stats(args):
    """데이터베이스 통계"""
    async def _stats():
        from kats.database.repository import Repository
        from kats.config.settings import Settings

        repo = Repository(Settings.DB_URL)
        await repo.init_db()

        print(f"\n{Color.bold('데이터베이스 통계')}")
        print("=" * 50)

        # 테이블별 행 수 조회
        from sqlalchemy import text
        async with repo.get_session() as session:
            tables = [
                ("stocks", "종목 마스터"),
                ("trades", "매매 내역"),
                ("trade_journal", "매매 일지"),
                ("strategies", "전략 마스터"),
                ("daily_stats", "일별 통계"),
                ("monthly_stats", "월별 통계"),
                ("drawdown_log", "드로우다운 이력"),
                ("system_config", "시스템 설정"),
                ("event_calendar", "이벤트 캘린더"),
                ("paper_account", "가상 계좌"),
            ]

            for table_name, description in tables:
                try:
                    result = await session.execute(
                        text(f"SELECT COUNT(*) FROM {table_name}")
                    )
                    count = result.scalar()
                    print(f"  {description:15s} ({table_name:20s}): {count:>6,}행")
                except Exception:
                    print(f"  {description:15s} ({table_name:20s}): {Color.warn('테이블 없음')}")

        print()
        db_file = PROJECT_DIR / "kats.db"
        if db_file.exists():
            size = db_file.stat().st_size
            print(f"  DB 크기: {size / 1024:.1f}KB")

    asyncio.run(_stats())


def cmd_redis_flush(args):
    """Redis → DB 수동 Flush"""
    if not is_redis_running():
        print(Color.err("Redis가 실행 중이 아닙니다."))
        sys.exit(1)

    target_date = args.date or datetime.now().strftime("%Y%m%d")
    print(f"{Color.info(f'Redis → DB Flush: {target_date}')}")

    async def _flush():
        from kats.database.repository import Repository
        from kats.database.redis_buffer import RedisTickBuffer
        from kats.config.settings import Settings

        repo = Repository(Settings.DB_URL)
        await repo.init_db()

        buffer = RedisTickBuffer(Settings.REDIS_URL)
        async with repo.get_session() as session:
            count = await buffer.flush_to_db(target_date, session)
        print(f"{Color.ok(f'Flush 완료: {count}건')}")

    asyncio.run(_flush())


def cmd_config(args):
    """시스템 설정 조회/변경"""
    async def _config():
        from kats.database.repository import Repository
        from kats.config.settings import Settings

        repo = Repository(Settings.DB_URL)
        await repo.init_db()

        if args.key and args.value:
            # 설정 변경
            await repo.set_system_config(args.key, args.value)
            print(f"{Color.ok('설정 변경')}: {args.key} = {args.value}")
        elif args.key:
            # 특정 설정 조회
            value = await repo.get_system_config(args.key)
            if value is not None:
                print(f"  {args.key} = {value}")
            else:
                print(f"  {Color.warn(f'{args.key}: 설정 없음')}")
        else:
            # 전체 설정 조회
            print(f"\n{Color.bold('시스템 설정')}")
            print("=" * 55)
            # .env 기반 설정
            print(f"\n  {Color.info('[환경 변수 (.env)]')}")
            print(f"  TRADE_MODE         = {Settings.TRADE_MODE}")
            print(f"  TOTAL_CAPITAL      = {Settings.TOTAL_CAPITAL:,}원")
            print(f"  RISK_PER_TRADE     = {Settings.RISK_PER_TRADE * 100:.1f}%")
            print(f"  DAILY_LOSS_LIMIT   = {Settings.DAILY_LOSS_LIMIT * 100:.1f}%")
            print(f"  MONTHLY_LOSS_LIMIT = {Settings.MONTHLY_LOSS_LIMIT * 100:.1f}%")
            print(f"  MAX_POSITIONS      = {Settings.MAX_POSITIONS}")
            print(f"  DB_URL             = {Settings.DB_URL}")
            print(f"  REDIS_URL          = {Settings.REDIS_URL}")

            # DB 저장 설정
            print(f"\n  {Color.info('[DB 저장 설정]')}")
            from sqlalchemy import text
            async with repo.get_session() as session:
                try:
                    result = await session.execute(
                        text("SELECT config_key, config_value, description FROM system_config ORDER BY config_key")
                    )
                    rows = result.fetchall()
                    for key, value, desc in rows:
                        desc_str = f"  # {desc}" if desc else ""
                        print(f"  {key:30s} = {value}{Color.DIM}{desc_str}{Color.NC}")
                except Exception:
                    print(f"  {Color.warn('system_config 테이블 없음')}")
            print()

    asyncio.run(_config())


# ══════════════════════════════════════════════════════════════════════════
#  CLI 진입점
# ══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="KATS v1.1 — 시스템 관리 CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
명령어 예시:
  kats_ctl.py start              # 모의 매매 모드로 시작
  kats_ctl.py start --live       # 실전 매매 모드로 시작
  kats_ctl.py stop               # KATS만 중지 (Redis 유지)
  kats_ctl.py stop --all         # KATS + Redis 모두 중지
  kats_ctl.py restart             # 재시작
  kats_ctl.py status              # 상태 대시보드
  kats_ctl.py status --json       # JSON 형식 상태
  kats_ctl.py logs --follow       # 실시간 로그 보기
  kats_ctl.py health              # 상세 헬스 체크
  kats_ctl.py db-init             # 데이터베이스 초기화
  kats_ctl.py db-stats            # DB 테이블 통계
  kats_ctl.py config              # 전체 설정 보기
  kats_ctl.py config trade_mode   # 특정 설정 보기
  kats_ctl.py config trade_mode PAPER  # 설정 변경
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="실행할 명령")

    # start
    p_start = subparsers.add_parser("start", help="시스템 시작")
    p_start.add_argument("--live", action="store_true", help="실전 매매 모드")
    p_start.add_argument("--skip-redis", action="store_true", help="Redis 시작 건너뛰기")

    # stop
    p_stop = subparsers.add_parser("stop", help="시스템 중지")
    p_stop.add_argument("--all", action="store_true", help="Redis도 함께 중지")
    p_stop.add_argument("--force", action="store_true", help="강제 종료")

    # restart
    p_restart = subparsers.add_parser("restart", help="시스템 재시작")
    p_restart.add_argument("--live", action="store_true", help="실전 매매 모드")
    p_restart.add_argument("--force", action="store_true", help="강제 종료 후 재시작")

    # status
    p_status = subparsers.add_parser("status", help="시스템 상태 확인")
    p_status.add_argument("--json", action="store_true", help="JSON 형식 출력")

    # logs
    p_logs = subparsers.add_parser("logs", help="로그 보기")
    p_logs.add_argument("--tail", "-n", type=int, default=50, help="출력 줄 수 (기본: 50)")
    p_logs.add_argument("--follow", "-f", action="store_true", help="실시간 추적")

    # health
    subparsers.add_parser("health", help="상세 헬스 체크")

    # db-init
    subparsers.add_parser("db-init", help="데이터베이스 초기화")

    # db-stats
    subparsers.add_parser("db-stats", help="데이터베이스 통계")

    # redis-flush
    p_flush = subparsers.add_parser("redis-flush", help="Redis → DB 수동 Flush")
    p_flush.add_argument("--date", help="대상 날짜 (YYYYMMDD, 기본: 오늘)")

    # config
    p_config = subparsers.add_parser("config", help="시스템 설정 조회/변경")
    p_config.add_argument("key", nargs="?", help="설정 키")
    p_config.add_argument("value", nargs="?", help="설정 값 (변경 시)")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    command_map = {
        "start": cmd_start,
        "stop": cmd_stop,
        "restart": cmd_restart,
        "status": cmd_status,
        "logs": cmd_logs,
        "health": cmd_health,
        "db-init": cmd_db_init,
        "db-stats": cmd_db_stats,
        "redis-flush": cmd_redis_flush,
        "config": cmd_config,
    }

    handler = command_map.get(args.command)
    if handler:
        handler(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
