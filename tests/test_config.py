from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from mcp_logbench.config import AppConfig, ConfigError, load_config, resolve_env_vars

if TYPE_CHECKING:
    from pathlib import Path


def write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(content)
    return p


# --- Happy path ---


def test_load_valid_config(tmp_path: Path) -> None:
    cfg_path = write_yaml(
        tmp_path,
        """
axiom:
  sources:
    - name: prod
      token: mytoken
      org_id: org-1
      datasets:
        - logs
""",
    )
    config = load_config(cfg_path)
    assert isinstance(config, AppConfig)
    assert config.axiom.sources[0].name == "prod"
    assert config.axiom.sources[0].token == "mytoken"
    assert config.server.host == "0.0.0.0"
    assert config.server.port == 8080


def test_defaults_applied(tmp_path: Path) -> None:
    cfg_path = write_yaml(
        tmp_path,
        """
axiom:
  sources:
    - name: prod
      token: tok
      org_id: org-1
      datasets: [logs]
""",
    )
    config = load_config(cfg_path)
    assert config.server.port == 8080
    assert config.axiom.rate_limit.queries_per_minute == 10
    assert config.axiom.rate_limit.burst == 3
    assert config.auth.provider == "azure_entra"


# --- Env var resolution ---


def test_env_var_resolution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_TOKEN", "secret-value")
    cfg_path = write_yaml(
        tmp_path,
        """
axiom:
  sources:
    - name: prod
      token: "${MY_TOKEN}"
      org_id: org-1
      datasets: [logs]
""",
    )
    config = load_config(cfg_path)
    assert config.axiom.sources[0].token == "secret-value"


def test_env_var_partial_substitution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_TOKEN", "abc123")
    cfg_path = write_yaml(
        tmp_path,
        """
axiom:
  sources:
    - name: prod
      token: "Bearer ${MY_TOKEN}"
      org_id: org-1
      datasets: [logs]
""",
    )
    config = load_config(cfg_path)
    assert config.axiom.sources[0].token == "Bearer abc123"


def test_missing_env_var_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING_VAR", raising=False)
    cfg_path = write_yaml(
        tmp_path,
        """
axiom:
  sources:
    - name: prod
      token: "${MISSING_VAR}"
      org_id: org-1
      datasets: [logs]
""",
    )
    with pytest.raises(ConfigError, match="MISSING_VAR"):
        load_config(cfg_path)


def test_resolve_env_vars_non_string(monkeypatch: pytest.MonkeyPatch) -> None:
    data: dict[str, Any] = {"count": 5, "flag": True, "nothing": None}
    result = resolve_env_vars(data)
    assert result == {"count": 5, "flag": True, "nothing": None}


# --- Validation errors ---


def test_empty_token_raises(tmp_path: Path) -> None:
    cfg_path = write_yaml(
        tmp_path,
        """
axiom:
  sources:
    - name: prod
      token: "   "
      org_id: org-1
      datasets: [logs]
""",
    )
    with pytest.raises(ConfigError, match="validation failed"):
        load_config(cfg_path)


def test_empty_datasets_raises(tmp_path: Path) -> None:
    cfg_path = write_yaml(
        tmp_path,
        """
axiom:
  sources:
    - name: prod
      token: tok
      org_id: org-1
      datasets: []
""",
    )
    with pytest.raises(ConfigError, match="validation failed"):
        load_config(cfg_path)


def test_duplicate_datasets_raises(tmp_path: Path) -> None:
    cfg_path = write_yaml(
        tmp_path,
        """
axiom:
  sources:
    - name: src1
      token: tok1
      org_id: org-1
      datasets: [shared-logs]
    - name: src2
      token: tok2
      org_id: org-1
      datasets: [shared-logs]
""",
    )
    with pytest.raises(ConfigError, match="validation failed"):
        load_config(cfg_path)


def test_dataset_name_with_invalid_chars_rejected(tmp_path: Path) -> None:
    cfg_path = write_yaml(
        tmp_path,
        """
axiom:
  sources:
    - name: prod
      token: tok
      org_id: org-1
      datasets: ["../etc/passwd"]
""",
    )
    with pytest.raises(ConfigError, match="validation failed"):
        load_config(cfg_path)


def test_source_name_validation(tmp_path: Path) -> None:
    cfg_path = write_yaml(
        tmp_path,
        """
axiom:
  sources:
    - name: "invalid name!"
      token: tok
      org_id: org-1
      datasets: [logs]
""",
    )
    with pytest.raises(ConfigError, match="validation failed"):
        load_config(cfg_path)


# --- File loading errors ---


def test_missing_config_file_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nonexistent.yaml")


def test_invalid_yaml_raises(tmp_path: Path) -> None:
    cfg_path = tmp_path / "bad.yaml"
    cfg_path.write_text("key: [unclosed")
    with pytest.raises(ConfigError, match="Invalid YAML"):
        load_config(cfg_path)


def test_config_path_from_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg_path = write_yaml(
        tmp_path,
        """
axiom:
  sources:
    - name: prod
      token: tok
      org_id: org-1
      datasets: [logs]
""",
    )
    monkeypatch.setenv("CONFIG_PATH", str(cfg_path))
    config = load_config()
    assert config.axiom.sources[0].name == "prod"
