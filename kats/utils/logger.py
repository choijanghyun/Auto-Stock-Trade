"""
KATS Structured Logging Configuration

Configures structlog for consistent, structured logging across the entire
KATS system. Provides both human-readable console output (with colors) and
machine-parseable JSON file output.

Usage:
    from kats.utils.logger import setup_logging, get_logger

    setup_logging(level="INFO")
    logger = get_logger("my_module")
    logger.info("order_submitted", stock_code="005930", quantity=10)
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import structlog


# ── Constants ────────────────────────────────────────────────────────────────

_LOG_DIR = Path("logs")
_DATE_FMT = "%Y-%m-%d %H:%M:%S"
_CONFIGURED = False


# ── Public API ───────────────────────────────────────────────────────────────

def setup_logging(level: str = "INFO") -> None:
    """Initialise structlog and stdlib logging for the KATS application.

    * Console output: colored, human-readable key=value format.
    * File output  : JSON lines written to ``logs/kats_{YYYY-MM-DD}.log``.

    This function is idempotent -- calling it more than once is safe but only
    the first invocation takes effect.

    Args:
        level: Root log level name (e.g. ``"DEBUG"``, ``"INFO"``, ``"WARNING"``).
    """
    global _CONFIGURED
    if _CONFIGURED:
        return
    _CONFIGURED = True

    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Ensure the log directory exists
    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    # ── File handler (JSON lines) ────────────────────────────────────────
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file = _LOG_DIR / f"kats_{today_str}.log"

    file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
    file_handler.setLevel(numeric_level)

    # ── Console handler (colored) ────────────────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)

    # ── Configure stdlib root logger ─────────────────────────────────────
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    # Remove any pre-existing handlers to avoid duplicate output
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # ── Shared structlog processors ──────────────────────────────────────
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt=_DATE_FMT, utc=False),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    # ── Formatter for file handler (JSON) ────────────────────────────────
    json_formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ],
        foreign_pre_chain=shared_processors,
    )
    file_handler.setFormatter(json_formatter)

    # ── Formatter for console handler (colored key=value) ────────────────
    console_formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer(colors=_supports_color()),
        ],
        foreign_pre_chain=shared_processors,
    )
    console_handler.setFormatter(console_formatter)

    # ── Configure structlog itself ───────────────────────────────────────
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a named, bound structlog logger.

    If ``setup_logging()`` has not been called yet, it is invoked
    automatically with default settings to guarantee safe usage from any
    import order.

    Args:
        name: Logger name, typically ``__name__`` of the calling module.

    Returns:
        A :class:`structlog.stdlib.BoundLogger` instance.
    """
    if not _CONFIGURED:
        setup_logging()
    return structlog.get_logger(name)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _supports_color() -> bool:
    """Detect whether the current terminal supports ANSI colors."""
    if os.getenv("NO_COLOR"):
        return False
    if os.getenv("FORCE_COLOR"):
        return True
    if not hasattr(sys.stdout, "isatty"):
        return False
    return sys.stdout.isatty()
