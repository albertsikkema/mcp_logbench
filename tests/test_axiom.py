from __future__ import annotations

from typing import TYPE_CHECKING, Any

import httpx
import pytest

from mcp_logbench.axiom import (
    AxiomAPIError,
    AxiomClient,
    AxiomConnectionError,
    DatasetNotFoundError,
)

if TYPE_CHECKING:
    import respx

    from mcp_logbench.config import AxiomConfig


# --- list_datasets ---


async def test_list_datasets_single_source(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets").respond(
        200,
        json=[
            {"name": "app-logs", "description": "Application logs"},
            {"name": "system-logs", "description": ""},
            {"name": "other-logs", "description": "Not in allowlist"},
        ],
    )
    respx_mock.get("https://axiom-staging.example.com/v1/datasets").respond(
        200,
        json=[{"name": "staging-logs", "description": "Staging logs"}],
    )

    async with AxiomClient(axiom_config) as client:
        result = await client.list_datasets()

    names = {d.name for d in result}
    assert names == {"app-logs", "system-logs", "staging-logs"}


async def test_list_datasets_multi_source(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets").respond(
        200,
        json=[{"name": "app-logs", "description": "prod"}],
    )
    respx_mock.get("https://axiom-staging.example.com/v1/datasets").respond(
        200,
        json=[{"name": "staging-logs", "description": "staging"}],
    )

    async with AxiomClient(axiom_config) as client:
        result = await client.list_datasets()

    sources = {d.source for d in result}
    assert "production" in sources
    assert "staging" in sources


async def test_list_datasets_filters_to_allowlist(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets").respond(
        200,
        json=[
            {"name": "app-logs", "description": ""},
            {"name": "not-allowed", "description": "Should be excluded"},
        ],
    )
    respx_mock.get("https://axiom-staging.example.com/v1/datasets").respond(
        200,
        json=[{"name": "staging-logs", "description": ""}],
    )

    async with AxiomClient(axiom_config) as client:
        result = await client.list_datasets()

    names = [d.name for d in result]
    assert "not-allowed" not in names


# --- get_dataset_schema ---


async def test_get_dataset_schema_happy_path(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets/app-logs").respond(
        200,
        json={
            "name": "app-logs",
            "fields": [
                {"name": "_time", "type": "datetime"},
                {"name": "message", "type": "string"},
                {"name": "level", "type": "string"},
            ],
        },
    )

    async with AxiomClient(axiom_config) as client:
        schema = await client.get_dataset_schema("app-logs")

    assert schema.dataset == "app-logs"
    assert len(schema.fields) == 3
    assert schema.fields[0].name == "_time"
    assert schema.fields[0].type == "datetime"


async def test_get_dataset_schema_unknown_dataset(axiom_config: AxiomConfig) -> None:
    async with AxiomClient(axiom_config) as client:
        with pytest.raises(DatasetNotFoundError, match="not in the configured allowlist"):
            await client.get_dataset_schema("nonexistent")


# --- query_apl ---


def _make_tabular_response(
    columns: list[str],
    rows: list[list[Any]],
    cursor: str | None = None,
) -> dict[str, Any]:
    return {
        "tables": [
            {
                "columns": [{"name": c} for c in columns],
                "rows": rows,
            }
        ],
        "status": {"minCursor": cursor} if cursor else {},
    }


async def test_query_apl_happy_path(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.post("https://axiom-prod.example.com/v1/datasets/_apl").respond(
        200,
        json=_make_tabular_response(
            columns=["_time", "message"],
            rows=[["2024-01-01T00:00:00Z", "hello"]],
        ),
    )

    async with AxiomClient(axiom_config) as client:
        result = await client.query_apl("app-logs", "['app-logs']")

    assert result.columns == ["_time", "message"]
    assert len(result.rows) == 1
    assert result.has_more is False


async def test_query_apl_default_time_range_appended(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    captured: list[dict[str, Any]] = []

    def capture_request(request: httpx.Request) -> httpx.Response:
        import json

        captured.append(json.loads(request.content))
        return httpx.Response(
            200, json=_make_tabular_response(columns=["_time"], rows=[])
        )

    respx_mock.post("https://axiom-prod.example.com/v1/datasets/_apl").mock(
        side_effect=capture_request
    )

    async with AxiomClient(axiom_config) as client:
        await client.query_apl("app-logs", "['app-logs']")

    assert len(captured) == 1
    assert "| where _time >= ago(7d)" in captured[0]["apl"]


async def test_query_apl_explicit_time_preserved(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    captured: list[dict[str, Any]] = []

    def capture_request(request: httpx.Request) -> httpx.Response:
        import json

        captured.append(json.loads(request.content))
        return httpx.Response(
            200, json=_make_tabular_response(columns=["_time"], rows=[])
        )

    respx_mock.post("https://axiom-prod.example.com/v1/datasets/_apl").mock(
        side_effect=capture_request
    )

    apl = "['app-logs'] | where _time > ago(1h)"
    async with AxiomClient(axiom_config) as client:
        await client.query_apl("app-logs", apl)

    assert captured[0]["apl"] == apl


async def test_query_apl_ago_keyword_preserved(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    captured: list[dict[str, Any]] = []

    def capture_request(request: httpx.Request) -> httpx.Response:
        import json

        captured.append(json.loads(request.content))
        return httpx.Response(
            200, json=_make_tabular_response(columns=["_time"], rows=[])
        )

    respx_mock.post("https://axiom-prod.example.com/v1/datasets/_apl").mock(
        side_effect=capture_request
    )

    apl = "['app-logs'] | where timestamp > ago(2h)"
    async with AxiomClient(axiom_config) as client:
        await client.query_apl("app-logs", apl)

    assert captured[0]["apl"] == apl


async def test_query_apl_datetime_keyword_preserved(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    captured: list[dict[str, Any]] = []

    def capture_request(request: httpx.Request) -> httpx.Response:
        import json

        captured.append(json.loads(request.content))
        return httpx.Response(
            200, json=_make_tabular_response(columns=["_time"], rows=[])
        )

    respx_mock.post("https://axiom-prod.example.com/v1/datasets/_apl").mock(
        side_effect=capture_request
    )

    apl = "['app-logs'] | where ts >= datetime(2024-01-01)"
    async with AxiomClient(axiom_config) as client:
        await client.query_apl("app-logs", apl)

    assert captured[0]["apl"] == apl


async def test_query_apl_pagination_returns_cursor(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    rows = [["row"] for _ in range(1001)]
    respx_mock.post("https://axiom-prod.example.com/v1/datasets/_apl").respond(
        200,
        json=_make_tabular_response(columns=["data"], rows=rows, cursor="cursor-abc"),
    )

    async with AxiomClient(axiom_config) as client:
        result = await client.query_apl("app-logs", "['app-logs'] | where _time > ago(1h)")

    assert result.has_more is True
    assert len(result.rows) == 1000


async def test_query_apl_result_size_limit(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    rows = [[f"row-{i}"] for i in range(1500)]
    respx_mock.post("https://axiom-prod.example.com/v1/datasets/_apl").respond(
        200,
        json=_make_tabular_response(columns=["data"], rows=rows),
    )

    async with AxiomClient(axiom_config) as client:
        result = await client.query_apl("app-logs", "['app-logs'] | where _time > ago(1h)")

    assert len(result.rows) == 1000
    assert result.has_more is True


async def test_query_apl_cursor_passthrough(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    captured: list[dict[str, Any]] = []

    def capture_request(request: httpx.Request) -> httpx.Response:
        import json

        captured.append(json.loads(request.content))
        return httpx.Response(
            200, json=_make_tabular_response(columns=["_time"], rows=[])
        )

    respx_mock.post("https://axiom-prod.example.com/v1/datasets/_apl").mock(
        side_effect=capture_request
    )

    async with AxiomClient(axiom_config) as client:
        await client.query_apl(
            "app-logs", "['app-logs'] | where _time > ago(1h)", cursor="my-cursor-123"
        )

    assert captured[0]["cursor"] == "my-cursor-123"


# --- Error handling ---


async def test_axiom_401_raises_api_error(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets/app-logs").respond(
        401, json={"error": "unauthorized"}
    )

    async with AxiomClient(axiom_config) as client:
        with pytest.raises(AxiomAPIError) as exc_info:
            await client.get_dataset_schema("app-logs")

    assert exc_info.value.status_code == 401
    assert "Authentication failed" in str(exc_info.value)
    assert "prod-secret-token" not in str(exc_info.value)


async def test_axiom_500_raises_api_error(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets/app-logs").respond(
        500, json={"error": "internal server error"}
    )

    async with AxiomClient(axiom_config) as client:
        with pytest.raises(AxiomAPIError) as exc_info:
            await client.get_dataset_schema("app-logs")

    assert exc_info.value.status_code == 500
    assert "500" in str(exc_info.value)
    assert "prod-secret-token" not in str(exc_info.value)


async def test_connection_error_raises_axiom_connection_error(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets/app-logs").mock(
        side_effect=httpx.ConnectError("connection refused")
    )

    async with AxiomClient(axiom_config) as client:
        with pytest.raises(AxiomConnectionError, match="Failed to connect"):
            await client.get_dataset_schema("app-logs")


async def test_read_timeout_raises_axiom_connection_error(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.post("https://axiom-prod.example.com/v1/datasets/_apl").mock(
        side_effect=httpx.ReadTimeout("timed out")
    )

    async with AxiomClient(axiom_config) as client:
        with pytest.raises(AxiomConnectionError, match="timed out"):
            await client.query_apl("app-logs", "['app-logs'] | where _time > ago(1h)")


async def test_token_never_in_error_messages(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets/app-logs").respond(
        500, json={"error": "internal"}
    )

    async with AxiomClient(axiom_config) as client:
        with pytest.raises(AxiomAPIError) as exc_info:
            await client.get_dataset_schema("app-logs")

    error_str = str(exc_info.value)
    for source in axiom_config.sources:
        assert source.token not in error_str


# --- Token redaction in logs ---


async def test_token_not_in_logs_on_api_error(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
    log_sink: list[str],
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets/app-logs").respond(
        403, json={"error": "forbidden"}
    )

    async with AxiomClient(axiom_config) as client:
        with pytest.raises(AxiomAPIError):
            await client.get_dataset_schema("app-logs")

    all_logs = " ".join(log_sink)
    for source in axiom_config.sources:
        assert source.token not in all_logs
    assert "production" in all_logs


# --- Edge cases ---


async def test_empty_query_response(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.post("https://axiom-prod.example.com/v1/datasets/_apl").respond(
        200, json={"tables": [], "status": {}}
    )

    async with AxiomClient(axiom_config) as client:
        result = await client.query_apl("app-logs", "['app-logs'] | where _time > ago(1h)")

    assert result.columns == []
    assert result.rows == []
    assert result.has_more is False
    assert result.cursor is None


async def test_dataset_with_hyphens_resolves(axiom_config: AxiomConfig) -> None:
    # "app-logs" and "system-logs" contain hyphens -- should resolve fine
    async with AxiomClient(axiom_config) as client:
        source = client._resolve_dataset("system-logs")
    assert source.name == "production"


# --- Security / audit logging ---


async def test_dataset_not_found_logs_warning(
    axiom_config: AxiomConfig,
    log_sink: list[str],
) -> None:
    async with AxiomClient(axiom_config) as client:
        with pytest.raises(DatasetNotFoundError):
            await client.get_dataset_schema("nonexistent")

    assert any("nonexistent" in msg for msg in log_sink)


async def test_list_datasets_partial_source_failure(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets").respond(
        500, json={"error": "unavailable"}
    )
    respx_mock.get("https://axiom-staging.example.com/v1/datasets").respond(
        200,
        json=[{"name": "staging-logs", "description": ""}],
    )

    async with AxiomClient(axiom_config) as client:
        result = await client.list_datasets()

    names = [d.name for d in result]
    assert "staging-logs" in names


async def test_malformed_json_on_200_raises_axiom_api_error(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets/app-logs").respond(
        200, content=b"not json"
    )

    async with AxiomClient(axiom_config) as client:
        with pytest.raises(AxiomAPIError, match="malformed response"):
            await client.get_dataset_schema("app-logs")


async def test_query_apl_truncation_logged(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
    log_sink: list[str],
) -> None:
    rows = [["row"] for _ in range(1001)]
    respx_mock.post("https://axiom-prod.example.com/v1/datasets/_apl").respond(
        200,
        json=_make_tabular_response(columns=["data"], rows=rows),
    )

    async with AxiomClient(axiom_config) as client:
        result = await client.query_apl("app-logs", "['app-logs'] | where _time > ago(1h)")

    assert result.has_more is True
    assert any("truncated" in msg for msg in log_sink)


async def test_request_error_detail_in_log(
    axiom_config: AxiomConfig,
    respx_mock: respx.MockRouter,
    log_sink: list[str],
) -> None:
    respx_mock.get("https://axiom-prod.example.com/v1/datasets/app-logs").mock(
        side_effect=httpx.ConnectError("connection refused by host")
    )

    async with AxiomClient(axiom_config) as client:
        with pytest.raises(AxiomConnectionError):
            await client.get_dataset_schema("app-logs")

    all_logs = " ".join(log_sink)
    assert "connection refused by host" in all_logs
