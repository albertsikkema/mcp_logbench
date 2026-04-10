from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from loguru import logger

from mcp_logbench.config import AppConfig, AxiomConfig, AxiomSourceConfig

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
def axiom_config() -> AxiomConfig:
    """AxiomConfig with two sources for multi-source testing."""
    return AxiomConfig(
        sources=[
            AxiomSourceConfig(
                name="production",
                url="https://axiom-prod.example.com",
                token="prod-secret-token",
                org_id="org-prod",
                datasets=["app-logs", "system-logs"],
            ),
            AxiomSourceConfig(
                name="staging",
                url="https://axiom-staging.example.com",
                token="staging-secret-token",
                org_id="org-staging",
                datasets=["staging-logs"],
            ),
        ]
    )


@pytest.fixture
def app_config(axiom_config: AxiomConfig) -> AppConfig:
    """Full AppConfig for server tests."""
    return AppConfig(axiom=axiom_config)


@pytest.fixture
def log_sink() -> Generator[list[str]]:
    """Capture Loguru output. Use instead of caplog (incompatible with Loguru)."""
    messages: list[str] = []
    logger.remove()
    logger.add(lambda m: messages.append(str(m)), format="{message} {extra}", level="DEBUG")
    yield messages
    logger.remove()
