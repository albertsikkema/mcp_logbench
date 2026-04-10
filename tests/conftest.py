from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from fastmcp.server.auth.providers.jwt import JWTVerifier, RSAKeyPair
from loguru import logger

from mcp_logbench.config import AppConfig, AuthConfig, AxiomConfig, AxiomSourceConfig

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

_TEST_TENANT = "test-tenant-id"
_TEST_CLIENT = "test-client-id"
_TEST_ISSUER = f"https://login.microsoftonline.com/{_TEST_TENANT}/v2.0"


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


@pytest.fixture(scope="module")
def rsa_key_pair() -> RSAKeyPair:
    return RSAKeyPair.generate()


@pytest.fixture
def make_token(rsa_key_pair: RSAKeyPair) -> Callable[..., str]:
    def _make(
        *,
        username: str = "testuser@example.com",
        oid: str = "test-oid-123",
        groups: list[str] | None = None,
        scopes: list[str] | None = None,
        expired: bool = False,
        tenant: str = _TEST_TENANT,
        audience: str = _TEST_CLIENT,
        extra_claims: dict[str, Any] | None = None,
    ) -> str:
        claims: dict[str, Any] = {
            "preferred_username": username,
            "oid": oid,
            "tid": tenant,
        }
        if groups is not None:
            claims["groups"] = groups
        if extra_claims:
            claims.update(extra_claims)
        return rsa_key_pair.create_token(
            subject=oid,
            issuer=f"https://login.microsoftonline.com/{tenant}/v2.0",
            audience=audience,
            scopes=scopes or [],
            expires_in_seconds=-1 if expired else 3600,
            additional_claims=claims,
        )

    return _make


@pytest.fixture
def auth_app_config(axiom_config: AxiomConfig) -> AppConfig:
    """AppConfig with auth enabled using test tenant/client."""
    return AppConfig(
        axiom=axiom_config,
        auth=AuthConfig(
            tenant_id=_TEST_TENANT,
            client_id=_TEST_CLIENT,
            base_url="https://test-server.example.com",
        ),
    )


@pytest.fixture
def auth_app_config_with_scope(axiom_config: AxiomConfig) -> AppConfig:
    """AppConfig with auth enabled and a required scope."""
    return AppConfig(
        axiom=axiom_config,
        auth=AuthConfig(
            tenant_id=_TEST_TENANT,
            client_id=_TEST_CLIENT,
            base_url="https://test-server.example.com",
            required_scope="access_as_user",
        ),
    )


@pytest.fixture
def auth_app_config_with_groups(axiom_config: AxiomConfig) -> AppConfig:
    """AppConfig with auth enabled and a required group."""
    return AppConfig(
        axiom=axiom_config,
        auth=AuthConfig(
            tenant_id=_TEST_TENANT,
            client_id=_TEST_CLIENT,
            base_url="https://test-server.example.com",
            required_groups=["allowed-group"],
        ),
    )


def build_auth_http_app(app_config: AppConfig, rsa_key_pair: RSAKeyPair):
    """Create a FastMCP HTTP app with auth backed by the test RSA key pair."""
    from fastmcp.server.auth import RemoteAuthProvider
    from pydantic import AnyHttpUrl

    from mcp_logbench.server import create_server

    verifier = JWTVerifier(
        public_key=rsa_key_pair.public_key,
        issuer=_TEST_ISSUER,
        audience=_TEST_CLIENT,
        required_scopes=(
            [app_config.auth.required_scope]
            if app_config.auth.required_scope
            else None
        ),
    )
    server = create_server(app_config)
    auth = RemoteAuthProvider(
        token_verifier=verifier,
        authorization_servers=[AnyHttpUrl(_TEST_ISSUER)],
        base_url="https://test-server.example.com",
    )
    server.auth = auth
    return server.http_app()


@pytest.fixture
def log_sink() -> Generator[list[str]]:
    """Capture Loguru output. Use instead of caplog (incompatible with Loguru)."""
    messages: list[str] = []
    logger.remove()
    logger.add(lambda m: messages.append(str(m)), format="{message} {extra}", level="DEBUG")
    yield messages
    logger.remove()
