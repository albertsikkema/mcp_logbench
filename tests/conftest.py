from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from loguru import logger

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def valid_config_data() -> dict[str, Any]:
    """Minimal valid config dict (after env var resolution)."""
    return {
        "server": {"host": "0.0.0.0", "port": 8080},
        "axiom": {
            "sources": [
                {
                    "name": "production",
                    "token": "test-token-value",
                    "org_id": "org-123",
                    "datasets": ["app-logs", "system-logs"],
                }
            ],
        },
    }


@pytest.fixture
def log_sink() -> Generator[list[str]]:
    """Capture Loguru output. Use instead of caplog (incompatible with Loguru)."""
    messages: list[str] = []
    logger.remove()
    logger.add(lambda m: messages.append(str(m)), format="{message}", level="DEBUG")
    yield messages
    logger.remove()
