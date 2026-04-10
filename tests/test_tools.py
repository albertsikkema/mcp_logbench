from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from mcp_logbench.server import create_server

if TYPE_CHECKING:
    import respx

    from mcp_logbench.config import AppConfig


@pytest.fixture
def mcp_server(app_config: AppConfig):
    return create_server(app_config)


# --- list_datasets ---


async def test_list_datasets(
    mcp_server,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets").respond(
        200, json=[{"name": "app-logs", "description": "App"}]
    )
    respx_mock.get("https://axiom-staging.example.com/v1/datasets").respond(
        200, json=[{"name": "staging-logs", "description": "Staging"}]
    )
    async with Client(mcp_server) as client:
        result = await client.call_tool("list_datasets", {})
    datasets = json.loads(result.content[0].text)
    names = {d["name"] for d in datasets}
    assert "app-logs" in names
    assert "staging-logs" in names


# --- get_dataset_schema ---


async def test_get_dataset_schema(
    mcp_server,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets/app-logs").respond(
        200, json={"fields": [{"name": "_time", "type": "datetime"}]}
    )
    async with Client(mcp_server) as client:
        result = await client.call_tool("get_dataset_schema", {"dataset": "app-logs"})
    assert result.data["dataset"] == "app-logs"
    assert len(result.data["fields"]) == 1


async def test_get_dataset_schema_unknown_dataset(mcp_server) -> None:
    async with Client(mcp_server) as client:
        with pytest.raises(ToolError, match="not in the configured allowlist"):
            await client.call_tool("get_dataset_schema", {"dataset": "nonexistent"})


# --- query_apl ---


async def test_query_apl(
    mcp_server,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.post(
        "https://axiom-prod.example.com/v1/datasets/_apl?format=tabular"
    ).respond(
        200,
        json={
            "tables": [{"columns": [{"name": "count"}], "rows": [[42]]}],
            "status": {},
        },
    )
    async with Client(mcp_server) as client:
        result = await client.call_tool(
            "query_apl",
            {"dataset": "app-logs", "apl": "['app-logs'] | count"},
        )
    assert result.data["columns"] == ["count"]
    assert result.data["rows"] == [[42]]


async def test_query_apl_empty_apl(mcp_server) -> None:
    async with Client(mcp_server) as client:
        with pytest.raises(ToolError, match="apl must not be empty"):
            await client.call_tool("query_apl", {"dataset": "app-logs", "apl": ""})


async def test_query_apl_unknown_dataset(mcp_server) -> None:
    async with Client(mcp_server) as client:
        with pytest.raises(ToolError, match="not in the configured allowlist"):
            await client.call_tool("query_apl", {"dataset": "nope", "apl": "count"})


# --- Rate limiting ---


async def test_query_apl_rate_limited(
    app_config: AppConfig,
    respx_mock: respx.MockRouter,
) -> None:
    from mcp_logbench.config import RateLimitConfig

    cfg = app_config.model_copy(deep=True)
    cfg.axiom.rate_limit = RateLimitConfig(queries_per_minute=60, burst=1)
    server = create_server(cfg)

    respx_mock.post(
        "https://axiom-prod.example.com/v1/datasets/_apl?format=tabular"
    ).respond(
        200,
        json={"tables": [{"columns": [{"name": "c"}], "rows": [[1]]}], "status": {}},
    )

    async with Client(server) as client:
        await client.call_tool("query_apl", {"dataset": "app-logs", "apl": "count"})
        with pytest.raises(ToolError, match="Rate limit exceeded"):
            await client.call_tool("query_apl", {"dataset": "app-logs", "apl": "count"})


# --- Audit logging ---


async def test_query_apl_audit_log(
    mcp_server,
    respx_mock: respx.MockRouter,
    log_sink: list[str],
) -> None:
    respx_mock.post(
        "https://axiom-prod.example.com/v1/datasets/_apl?format=tabular"
    ).respond(
        200,
        json={"tables": [{"columns": [{"name": "c"}], "rows": [[1]]}], "status": {}},
    )
    async with Client(mcp_server) as client:
        await client.call_tool("query_apl", {"dataset": "app-logs", "apl": "count"})

    audit_lines = [m for m in log_sink if "audit" in m]
    assert len(audit_lines) >= 1
    assert "app-logs" in audit_lines[0]
    assert "success" in audit_lines[0]


# --- Read-only invariant ---


async def test_only_three_tools_registered(mcp_server) -> None:
    async with Client(mcp_server) as client:
        tools = await client.list_tools()
    tool_names = {t.name for t in tools}
    assert tool_names == {"list_datasets", "get_dataset_schema", "query_apl"}
