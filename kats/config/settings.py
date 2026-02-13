"""
KATS Configuration Settings

Central configuration class that loads environment variables via python-dotenv.
All settings can be overridden through .env file or environment variables.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Application-wide configuration loaded from environment variables."""

    # ── KIS API Credentials ──────────────────────────────────────────
    KIS_APP_KEY: str = os.getenv("KIS_APP_KEY", "")
    KIS_APP_SECRET: str = os.getenv("KIS_APP_SECRET", "")
    KIS_ACCOUNT_NO: str = os.getenv("KIS_ACCOUNT_NO", "")
    KIS_ACCOUNT_PROD_CODE: str = os.getenv("KIS_ACCOUNT_PROD_CODE", "01")

    # ── Trade Mode ───────────────────────────────────────────────────
    TRADE_MODE: str = os.getenv("TRADE_MODE", "PAPER")  # LIVE / PAPER

    # ── Database ─────────────────────────────────────────────────────
    DB_TYPE: str = os.getenv("DB_TYPE", "sqlite")
    DB_URL: str = os.getenv("DB_URL", "sqlite+aiosqlite:///kats.db")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")

    # ── Risk Management ──────────────────────────────────────────────
    TOTAL_CAPITAL: int = int(os.getenv("TOTAL_CAPITAL", "100000000"))
    RISK_PER_TRADE: float = float(os.getenv("RISK_PER_TRADE", "0.02"))
    DAILY_LOSS_LIMIT: float = float(os.getenv("DAILY_LOSS_LIMIT", "0.03"))
    MONTHLY_LOSS_LIMIT: float = float(os.getenv("MONTHLY_LOSS_LIMIT", "0.06"))
    MAX_POSITIONS: int = int(os.getenv("MAX_POSITIONS", "5"))

    # ── Grade-Based Position Limits ──────────────────────────────────
    GRADE_A_MAX_PCT: float = float(os.getenv("GRADE_A_MAX_PCT", "30"))
    GRADE_B_MAX_PCT: float = float(os.getenv("GRADE_B_MAX_PCT", "20"))
    GRADE_C_MAX_PCT: float = float(os.getenv("GRADE_C_MAX_PCT", "10"))
    SECTOR_MAX_PCT: float = float(os.getenv("SECTOR_MAX_PCT", "40"))
    TRAILING_STOP_DEFAULT_PCT: float = float(
        os.getenv("TRAILING_STOP_DEFAULT_PCT", "5.0")
    )

    # ── Notification ─────────────────────────────────────────────────
    SLACK_WEBHOOK_URL: str = os.getenv("SLACK_WEBHOOK_URL", "")
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # ── KIS API Base URLs ────────────────────────────────────────────
    BASE_URL_LIVE: str = "https://openapi.koreainvestment.com:9443"
    BASE_URL_PAPER: str = "https://openapivts.koreainvestment.com:29443"
    WS_URL: str = "ws://ops.koreainvestment.com:21000"

    @classmethod
    def get_base_url(cls) -> str:
        """Return the appropriate base URL based on the current trade mode."""
        return cls.BASE_URL_LIVE if cls.TRADE_MODE == "LIVE" else cls.BASE_URL_PAPER
