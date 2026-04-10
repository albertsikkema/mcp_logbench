from __future__ import annotations

import time as time_mod
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from loguru import logger

from mcp_logbench.axiom import (
    AxiomClient,
    AxiomError,
    DatasetNotFoundError,
)
from mcp_logbench.rate_limit import RateLimiter

if TYPE_CHECKING:
    from mcp_logbench.config import AppConfig


def create_server(config: AppConfig) -> FastMCP:
    """Create and configure the FastMCP server with all tools."""
    client = AxiomClient(config.axiom)
    limiter = RateLimiter(config.axiom.rate_limit)

    @asynccontextmanager
    async def lifespan(app: Any):
        yield
        await client.aclose()

    mcp = FastMCP(
        "MCP LogBench",
        mask_error_details=True,
        lifespan=lifespan,
    )

    @mcp.tool
    async def list_datasets() -> list[dict[str, str]]:
        """List all queryable datasets across all configured sources.

        Returns dataset name, source label, and description for each dataset.
        """
        try:
            datasets = await client.list_datasets()
        except AxiomError as e:
            raise ToolError(str(e)) from e
        return [d.model_dump() for d in datasets]

    @mcp.tool
    async def get_dataset_schema(dataset: str) -> dict[str, Any]:
        """Get the field names and types for a dataset.

        Args:
            dataset: Name of the dataset to inspect.
        """
        if not dataset.strip():
            raise ToolError("dataset must not be empty")
        try:
            schema = await client.get_dataset_schema(dataset)
        except DatasetNotFoundError as e:
            raise ToolError(str(e)) from e
        except AxiomError as e:
            raise ToolError(str(e)) from e
        return schema.model_dump()

    @mcp.tool
    async def query_apl(
        dataset: str,
        apl: str,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Execute an APL query against a dataset and return results.

        Args:
            dataset: Name of the dataset to query.
            apl: APL query string.
            cursor: Pagination cursor from a previous query.
        """
        if not dataset.strip():
            raise ToolError("dataset must not be empty")
        if not apl.strip():
            raise ToolError("apl must not be empty")

        if not limiter.acquire():
            retry = limiter.retry_after()
            raise ToolError(f"Rate limit exceeded. Try again in {retry:.1f} seconds.")

        start = time_mod.monotonic()
        status = "success"
        result_rows = 0
        try:
            result = await client.query_apl(dataset, apl, cursor)
            result_rows = len(result.rows)
        except DatasetNotFoundError as e:
            status = "error"
            raise ToolError(str(e)) from e
        except AxiomError as e:
            status = "error"
            raise ToolError(str(e)) from e
        finally:
            duration_ms = (time_mod.monotonic() - start) * 1000
            logger.info(
                "audit: query executed",
                user="anonymous",
                user_oid="",
                dataset=dataset,
                apl_query=apl[:500],
                source=_resolve_source_name(client, dataset),
                duration_ms=round(duration_ms, 1),
                result_rows=result_rows,
                status=status,
            )
        return result.model_dump()

    return mcp


def _resolve_source_name(client: AxiomClient, dataset: str) -> str:
    """Resolve source name for audit log. Returns '' if not found."""
    source = client._dataset_map.get(dataset)
    return source.name if source else ""
