from __future__ import annotations

import re
import time as time_mod
import uuid
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

_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


def _sanitize_log_str(s: str, max_len: int = 500) -> str:
    """Replace control characters and truncate for safe log output."""
    return _CONTROL_RE.sub(" ", s[:max_len])


def create_server(config: AppConfig) -> FastMCP:
    """Create and configure the FastMCP server with all tools."""
    client = AxiomClient(config.axiom)
    limiter = RateLimiter(config.axiom.rate_limit)

    @asynccontextmanager
    async def lifespan(app: Any):
        logger.info("MCP LogBench server started")
        yield
        await client.aclose()
        logger.info("MCP LogBench server stopped")

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
        request_id = str(uuid.uuid4())
        start = time_mod.monotonic()
        status = "error"
        result_count = 0
        datasets: list[Any] = []
        try:
            datasets = await client.list_datasets()
            result_count = len(datasets)
            status = "success"
        except AxiomError as e:
            raise ToolError(str(e)) from e
        finally:
            duration_ms = (time_mod.monotonic() - start) * 1000
            log_fn = logger.warning if status == "error" else logger.info
            log_fn(
                "audit: list_datasets",
                request_id=request_id,
                user="anonymous",  # TODO(T-004): replace with authenticated user identity
                user_oid="",
                result_count=result_count,
                duration_ms=round(duration_ms, 1),
                status=status,
            )
        return [d.model_dump() for d in datasets]

    @mcp.tool
    async def get_dataset_schema(dataset: str) -> dict[str, Any]:
        """Get the field names and types for a dataset.

        Args:
            dataset: Name of the dataset to inspect.
        """
        if not dataset.strip():
            raise ToolError("dataset must not be empty")
        request_id = str(uuid.uuid4())
        start = time_mod.monotonic()
        status = "error"
        schema = None
        try:
            schema = await client.get_dataset_schema(dataset)
            status = "success"
        except DatasetNotFoundError as e:
            raise ToolError(str(e)) from e
        except AxiomError as e:
            raise ToolError(str(e)) from e
        finally:
            duration_ms = (time_mod.monotonic() - start) * 1000
            log_fn = logger.warning if status == "error" else logger.info
            log_fn(
                "audit: get_dataset_schema",
                request_id=request_id,
                user="anonymous",  # TODO(T-004): replace with authenticated user identity
                user_oid="",
                dataset=dataset,
                source=_resolve_source_name(client, dataset),
                duration_ms=round(duration_ms, 1),
                status=status,
            )
        assert schema is not None
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
            logger.warning(
                "Rate limit exceeded",
                dataset=dataset,
                retry_after_s=round(retry, 1),
            )
            raise ToolError(f"Rate limit exceeded. Try again in {retry:.1f} seconds.")

        request_id = str(uuid.uuid4())
        start = time_mod.monotonic()
        status = "error"
        result_rows = 0
        try:
            result = await client.query_apl(dataset, apl, cursor)
            result_rows = len(result.rows)
            status = "success"
        except DatasetNotFoundError as e:
            raise ToolError(str(e)) from e
        except AxiomError as e:
            raise ToolError(str(e)) from e
        finally:
            duration_ms = (time_mod.monotonic() - start) * 1000
            log_fn = logger.warning if status == "error" else logger.info
            log_fn(
                "audit: query executed",
                request_id=request_id,
                user="anonymous",  # TODO(T-004): replace with authenticated user identity
                user_oid="",
                dataset=dataset,
                apl_query=_sanitize_log_str(apl),
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
