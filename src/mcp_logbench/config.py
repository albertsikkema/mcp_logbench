"""Configuration loading: YAML parsing, env var resolution, Pydantic validation."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, ValidationError, field_validator, model_validator

ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")


class ConfigError(Exception):
    """Raised when configuration is invalid or cannot be loaded."""


# --- Env var resolution ---


def _resolve_string(value: str) -> str:
    """Resolve ${VAR_NAME} references in a single string value."""

    def replacer(match: re.Match[str]) -> str:
        var_name = match.group(1)
        env_value = os.environ.get(var_name)
        if env_value is None:
            raise ConfigError(f"Environment variable {var_name} is not set")
        return env_value

    return ENV_VAR_PATTERN.sub(replacer, value)


def resolve_env_vars(data: Any) -> Any:
    """Recursively resolve ${VAR_NAME} patterns in parsed YAML data."""
    if isinstance(data, str):
        return _resolve_string(data)
    if isinstance(data, dict):
        return {k: resolve_env_vars(v) for k, v in data.items()}
    if isinstance(data, list):
        return [resolve_env_vars(item) for item in data]
    return data


# --- Pydantic models ---


class ServerConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8080


class AxiomSourceConfig(BaseModel):
    name: str
    url: str = "https://api.axiom.co"
    token: str
    org_id: str
    datasets: list[str]

    @field_validator("token")
    @classmethod
    def token_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("token must not be empty")
        return v

    @field_validator("datasets")
    @classmethod
    def datasets_not_empty(cls, v: list[str]) -> list[str]:
        if len(v) == 0:
            raise ValueError("at least one dataset is required")
        for name in v:
            if not re.match(r"^[a-zA-Z0-9_-]+$", name):
                raise ValueError(
                    f"dataset name '{name}' must contain only alphanumeric"
                    " characters, hyphens, and underscores"
                )
        return v

    @field_validator("name")
    @classmethod
    def name_valid(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError(
                "name must contain only alphanumeric characters, hyphens, and underscores"
            )
        return v


class RateLimitConfig(BaseModel):
    queries_per_minute: int = 10
    burst: int = 3


class AxiomConfig(BaseModel):
    sources: list[AxiomSourceConfig]
    rate_limit: RateLimitConfig = RateLimitConfig()

    @model_validator(mode="after")
    def no_duplicate_datasets(self) -> Self:
        seen: set[str] = set()
        for source in self.sources:
            for ds in source.datasets:
                if ds in seen:
                    raise ValueError(f"duplicate dataset '{ds}' across sources")
                seen.add(ds)
        return self


class AuthConfig(BaseModel):
    provider: Literal["azure_entra"] = "azure_entra"
    tenant_id: str = ""
    client_id: str = ""
    base_url: str = ""
    required_scope: str | None = None
    required_groups: list[str] = []

    @field_validator("base_url")
    @classmethod
    def base_url_must_be_https(cls, v: str) -> str:
        if v and not v.startswith("https://"):
            raise ValueError("base_url must use HTTPS")
        return v

    @model_validator(mode="after")
    def auth_fields_consistent(self) -> Self:
        has_tenant = bool(self.tenant_id)
        has_client = bool(self.client_id)
        has_base_url = bool(self.base_url)
        if has_tenant != has_client:
            raise ValueError("tenant_id and client_id must both be set or both be empty")
        if has_tenant and not has_base_url:
            raise ValueError("base_url is required when authentication is enabled")
        return self


class AppConfig(BaseModel):
    server: ServerConfig = ServerConfig()
    axiom: AxiomConfig
    auth: AuthConfig = AuthConfig()


# --- Loading ---


def load_config(path: str | Path | None = None) -> AppConfig:
    """Load and validate configuration from a YAML file.

    Args:
        path: Path to the YAML config file. Defaults to CONFIG_PATH env var
              or 'config.yaml'.

    Raises:
        ConfigError: If the file cannot be read, env vars are missing,
                     or validation fails.
    """
    if path is None:
        path = os.environ.get("CONFIG_PATH", "config.yaml")
    config_path = Path(path)

    if not config_path.exists():
        raise ConfigError(f"Configuration file not found: {config_path}")

    try:
        raw = config_path.read_text()
        data = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in {config_path}: {e}") from e

    if not isinstance(data, dict):
        raise ConfigError(
            f"Configuration file must contain a YAML mapping, got {type(data).__name__}"
        )

    try:
        resolved = resolve_env_vars(data)
    except ConfigError:
        raise
    except Exception as e:
        raise ConfigError(f"Error resolving environment variables: {e}") from e

    try:
        return AppConfig.model_validate(resolved)
    except ValidationError as e:
        errors = []
        for err in e.errors():
            loc = " -> ".join(str(part) for part in err["loc"])
            errors.append(f"  {loc}: {err['msg']}")
        detail = "\n".join(errors)
        raise ConfigError(f"Configuration validation failed:\n{detail}") from None
