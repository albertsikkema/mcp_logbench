from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from mcp_logbench.logging import setup_logging

if TYPE_CHECKING:
    import pytest


def test_setup_json_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    setup_logging(log_format="json")
    # Verify a handler is registered (logger has internal _core with handlers)
    assert len(logger._core.handlers) >= 1  # type: ignore[attr-defined]


def test_setup_text_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    setup_logging(log_format="text")
    assert len(logger._core.handlers) >= 1  # type: ignore[attr-defined]


def test_env_var_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOG_FORMAT", "text")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    # Should not raise
    setup_logging(log_format="json", log_level="INFO")
    assert len(logger._core.handlers) >= 1  # type: ignore[attr-defined]
