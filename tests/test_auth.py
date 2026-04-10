"""Auth integration tests: JWT validation, user identity, group enforcement."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.exceptions import ToolError
from starlette.testclient import TestClient

from tests.conftest import build_auth_http_app

if TYPE_CHECKING:
    import respx
    from fastmcp.server.auth.providers.jwt import RSAKeyPair

    from mcp_logbench.config import AppConfig


def _asgi_client_factory(app):
    """Return an httpx_client_factory that uses ASGITransport for the given app."""

    def factory(**kwargs):
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            **kwargs,
        )

    return factory


# --- 401 / 403 rejection tests (HTTP level) ---


async def test_no_token_rejected(
    auth_app_config: AppConfig,
    rsa_key_pair: RSAKeyPair,
) -> None:
    app = build_auth_http_app(auth_app_config, rsa_key_pair)
    with TestClient(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as c:
            r = await c.post("/mcp", json={})
    assert r.status_code == 401


async def test_expired_token_rejected(
    auth_app_config: AppConfig,
    rsa_key_pair: RSAKeyPair,
    make_token,
) -> None:
    app = build_auth_http_app(auth_app_config, rsa_key_pair)
    token = make_token(expired=True)
    with TestClient(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as c:
            r = await c.post(
                "/mcp",
                json={},
                headers={"Authorization": f"Bearer {token}"},
            )
    assert r.status_code == 401


async def test_wrong_tenant_rejected(
    auth_app_config: AppConfig,
    rsa_key_pair: RSAKeyPair,
    make_token,
) -> None:
    app = build_auth_http_app(auth_app_config, rsa_key_pair)
    token = make_token(tenant="wrong-tenant-id")
    with TestClient(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as c:
            r = await c.post(
                "/mcp",
                json={},
                headers={"Authorization": f"Bearer {token}"},
            )
    assert r.status_code == 401


async def test_wrong_audience_rejected(
    auth_app_config: AppConfig,
    rsa_key_pair: RSAKeyPair,
    make_token,
) -> None:
    app = build_auth_http_app(auth_app_config, rsa_key_pair)
    token = make_token(audience="wrong-client-id")
    with TestClient(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as c:
            r = await c.post(
                "/mcp",
                json={},
                headers={"Authorization": f"Bearer {token}"},
            )
    assert r.status_code == 401


async def test_missing_scope_rejected(
    auth_app_config_with_scope: AppConfig,
    rsa_key_pair: RSAKeyPair,
    make_token,
) -> None:
    app = build_auth_http_app(auth_app_config_with_scope, rsa_key_pair)
    token = make_token(scopes=[])  # no scope provided
    with TestClient(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as c:
            r = await c.post(
                "/mcp",
                json={},
                headers={"Authorization": f"Bearer {token}"},
            )
    assert r.status_code in (401, 403)


# --- Valid token allows tool access ---


async def test_valid_token_allows_tool_access(
    auth_app_config: AppConfig,
    rsa_key_pair: RSAKeyPair,
    make_token,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets").respond(
        200, json=[{"name": "app-logs", "description": "App"}]
    )
    respx_mock.get("https://axiom-staging.example.com/v1/datasets").respond(
        200, json=[{"name": "staging-logs", "description": "Staging"}]
    )

    app = build_auth_http_app(auth_app_config, rsa_key_pair)
    token = make_token()

    with TestClient(app):
        transport = StreamableHttpTransport(
            url="http://testserver/mcp",
            auth=token,
            httpx_client_factory=_asgi_client_factory(app),
        )
        async with Client(transport) as client:
            result = await client.call_tool("list_datasets", {})

    datasets = json.loads(result.content[0].text)
    assert any(d["name"] == "app-logs" for d in datasets)


async def test_valid_scope_allowed(
    auth_app_config_with_scope: AppConfig,
    rsa_key_pair: RSAKeyPair,
    make_token,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets").respond(
        200, json=[{"name": "app-logs", "description": "App"}]
    )
    respx_mock.get("https://axiom-staging.example.com/v1/datasets").respond(
        200, json=[{"name": "staging-logs", "description": "Staging"}]
    )

    app = build_auth_http_app(auth_app_config_with_scope, rsa_key_pair)
    token = make_token(scopes=["access_as_user"])

    with TestClient(app):
        transport = StreamableHttpTransport(
            url="http://testserver/mcp",
            auth=token,
            httpx_client_factory=_asgi_client_factory(app),
        )
        async with Client(transport) as client:
            result = await client.call_tool("list_datasets", {})

    datasets = json.loads(result.content[0].text)
    assert len(datasets) >= 1


# --- User identity in audit logs ---


async def test_valid_token_audit_log_shows_user(
    auth_app_config: AppConfig,
    rsa_key_pair: RSAKeyPair,
    make_token,
    log_sink: list[str],
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets").respond(
        200, json=[{"name": "app-logs", "description": "App"}]
    )
    respx_mock.get("https://axiom-staging.example.com/v1/datasets").respond(
        200, json=[{"name": "staging-logs", "description": "Staging"}]
    )

    app = build_auth_http_app(auth_app_config, rsa_key_pair)
    token = make_token(username="alice@example.com", oid="oid-alice-123")

    with TestClient(app):
        transport = StreamableHttpTransport(
            url="http://testserver/mcp",
            auth=token,
            httpx_client_factory=_asgi_client_factory(app),
        )
        async with Client(transport) as client:
            await client.call_tool("list_datasets", {})

    audit_lines = [m for m in log_sink if "audit" in m]
    assert len(audit_lines) >= 1
    assert "alice@example.com" in audit_lines[0]
    assert "oid-alice-123" in audit_lines[0]


# --- Group enforcement ---


async def test_group_check_passes(
    auth_app_config_with_groups: AppConfig,
    rsa_key_pair: RSAKeyPair,
    make_token,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets").respond(
        200, json=[{"name": "app-logs", "description": "App"}]
    )
    respx_mock.get("https://axiom-staging.example.com/v1/datasets").respond(
        200, json=[{"name": "staging-logs", "description": "Staging"}]
    )

    app = build_auth_http_app(auth_app_config_with_groups, rsa_key_pair)
    token = make_token(groups=["allowed-group", "other-group"])

    with TestClient(app):
        transport = StreamableHttpTransport(
            url="http://testserver/mcp",
            auth=token,
            httpx_client_factory=_asgi_client_factory(app),
        )
        async with Client(transport) as client:
            result = await client.call_tool("list_datasets", {})

    datasets = json.loads(result.content[0].text)
    assert len(datasets) >= 1


async def test_group_check_fails(
    auth_app_config_with_groups: AppConfig,
    rsa_key_pair: RSAKeyPair,
    make_token,
) -> None:
    app = build_auth_http_app(auth_app_config_with_groups, rsa_key_pair)
    token = make_token(groups=["other-group"])  # not in required group

    with TestClient(app):
        transport = StreamableHttpTransport(
            url="http://testserver/mcp",
            auth=token,
            httpx_client_factory=_asgi_client_factory(app),
        )
        async with Client(transport) as client:
            with pytest.raises(ToolError, match="Access denied"):
                await client.call_tool("list_datasets", {})


async def test_no_groups_claim_rejected(
    auth_app_config_with_groups: AppConfig,
    rsa_key_pair: RSAKeyPair,
    make_token,
) -> None:
    app = build_auth_http_app(auth_app_config_with_groups, rsa_key_pair)
    token = make_token()  # no groups claim

    with TestClient(app):
        transport = StreamableHttpTransport(
            url="http://testserver/mcp",
            auth=token,
            httpx_client_factory=_asgi_client_factory(app),
        )
        async with Client(transport) as client:
            with pytest.raises(ToolError, match="Access denied"):
                await client.call_tool("list_datasets", {})


# --- Auth disabled fallback ---


async def test_auth_disabled_anonymous(
    app_config: AppConfig,
    respx_mock: respx.MockRouter,
    log_sink: list[str],
) -> None:
    """When auth is disabled, tools work and user is 'anonymous' in logs."""
    from mcp_logbench.server import create_server

    respx_mock.get("https://axiom-prod.example.com/v1/datasets").respond(
        200, json=[{"name": "app-logs", "description": "App"}]
    )
    respx_mock.get("https://axiom-staging.example.com/v1/datasets").respond(
        200, json=[{"name": "staging-logs", "description": "Staging"}]
    )

    server = create_server(app_config)
    async with Client(server) as client:
        result = await client.call_tool("list_datasets", {})

    datasets = json.loads(result.content[0].text)
    assert len(datasets) >= 1

    audit_lines = [m for m in log_sink if "audit" in m]
    assert len(audit_lines) >= 1
    assert "anonymous" in audit_lines[0]


# --- OAuth metadata advertisement ---


async def test_oauth_metadata_advertised(
    auth_app_config: AppConfig,
    rsa_key_pair: RSAKeyPair,
) -> None:
    """OAuth protected-resource metadata is available for MCP client discovery."""
    app = build_auth_http_app(auth_app_config, rsa_key_pair)
    with TestClient(app) as tc:
        r = tc.get("/.well-known/oauth-protected-resource/mcp")
    assert r.status_code == 200
    data = r.json()
    assert "authorization_servers" in data or "resource" in data
