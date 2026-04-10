"""Structured logging configuration using Loguru."""

from __future__ import annotations

import os
import sys

from loguru import logger


def setup_logging(
    log_level: str = "INFO",
    log_format: str = "json",
) -> None:
    """Configure Loguru for structured logging.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
                   Defaults to LOG_LEVEL env var or INFO.
        log_format: Output format -- 'json' for production (stdout),
                    'text' for development (stderr). Defaults to
                    LOG_FORMAT env var or 'json'.
    """
    level = os.environ.get("LOG_LEVEL", log_level).upper()
    fmt = os.environ.get("LOG_FORMAT", log_format).lower()

    logger.remove()

    if fmt == "json":
        logger.add(
            sys.stdout,
            serialize=True,
            level=level,
            diagnose=False,
        )
    else:
        logger.add(
            sys.stderr,
            level=level,
            colorize=True,
            diagnose=False,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level:<8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                "<level>{message}</level>"
            ),
        )
