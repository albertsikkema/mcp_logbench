from __future__ import annotations

from typing import TYPE_CHECKING, Any

import httpx
from loguru import logger

from mcp_logbench.models import DatasetInfo, DatasetSchema, FieldInfo, QueryResult

if TYPE_CHECKING:
    from mcp_logbench.config import AxiomConfig, AxiomSourceConfig


# --- Errors ---


class AxiomError(Exception):
    """Base exception for Axiom client errors."""


class DatasetNotFoundError(AxiomError):
    """Raised when a dataset is not in the configured allowlist."""


class AxiomAPIError(AxiomError):
    """Raised when the Axiom API returns an error response."""

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class AxiomConnectionError(AxiomError):
    """Raised when the Axiom API is unreachable or times out."""


# --- Constants ---

_CONNECT_TIMEOUT = 5.0
_READ_TIMEOUT = 30.0
_DEFAULT_MAX_RESULTS = 1000

# Keywords indicating the APL already has a time filter
_TIME_KEYWORDS = ("_time", "ago(", "datetime(")


# --- Client ---


class AxiomClient:
    """HTTP client for the Axiom API. Proxies queries using per-source credentials."""

    def __init__(self, config: AxiomConfig) -> None:
        self._config = config
        # Build dataset -> source lookup (uniqueness guaranteed by AxiomConfig validator)
        self._dataset_map: dict[str, AxiomSourceConfig] = {}
        for source in config.sources:
            for ds in source.datasets:
                self._dataset_map[ds] = source

        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(_READ_TIMEOUT, connect=_CONNECT_TIMEOUT),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> AxiomClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    def _resolve_dataset(self, dataset: str) -> AxiomSourceConfig:
        source = self._dataset_map.get(dataset)
        if source is None:
            raise DatasetNotFoundError(
                f"Dataset '{dataset}' is not in the configured allowlist"
            )
        return source

    @staticmethod
    def _headers(source: AxiomSourceConfig) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {source.token}",
            "X-Axiom-Org-Id": source.org_id,
        }

    async def list_datasets(self) -> list[DatasetInfo]:
        results: list[DatasetInfo] = []
        for source in self._config.sources:
            try:
                resp = await self._http.get(
                    f"{source.url}/v1/datasets",
                    headers=self._headers(source),
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                raise self._translate_api_error(e, source.name) from None
            except httpx.RequestError as e:
                raise self._translate_request_error(e, source.name) from None

            for ds in resp.json():
                if ds["name"] in source.datasets:
                    results.append(
                        DatasetInfo(
                            name=ds["name"],
                            source=source.name,
                            description=ds.get("description") or "",
                        )
                    )
        return results

    async def get_dataset_schema(self, dataset: str) -> DatasetSchema:
        source = self._resolve_dataset(dataset)
        try:
            resp = await self._http.get(
                f"{source.url}/v1/datasets/{dataset}",
                headers=self._headers(source),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise self._translate_api_error(e, source.name) from None
        except httpx.RequestError as e:
            raise self._translate_request_error(e, source.name) from None

        data = resp.json()
        fields = [FieldInfo(name=f["name"], type=f["type"]) for f in data.get("fields", [])]
        return DatasetSchema(dataset=dataset, fields=fields)

    async def query_apl(
        self,
        dataset: str,
        apl: str,
        cursor: str | None = None,
    ) -> QueryResult:
        source = self._resolve_dataset(dataset)
        apl = self._apply_time_default(apl)

        body: dict[str, Any] = {"apl": apl}
        if cursor is not None:
            body["cursor"] = cursor

        try:
            resp = await self._http.post(
                f"{source.url}/v1/datasets/_apl?format=tabular",
                headers=self._headers(source),
                json=body,
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise self._translate_api_error(e, source.name) from None
        except httpx.RequestError as e:
            raise self._translate_request_error(e, source.name) from None

        return self._parse_query_response(resp.json())

    @staticmethod
    def _apply_time_default(apl: str) -> str:
        apl_lower = apl.lower()
        for keyword in _TIME_KEYWORDS:
            if keyword in apl_lower:
                return apl
        return f"{apl} | where _time >= ago(7d)"

    def _parse_query_response(self, data: dict[str, Any]) -> QueryResult:
        tables = data.get("tables", [])
        if not tables:
            return QueryResult(columns=[], rows=[])

        table = tables[0]
        columns = [col["name"] for col in table.get("columns", [])]
        all_rows = table.get("rows") or []

        has_more = len(all_rows) > _DEFAULT_MAX_RESULTS
        rows = all_rows[:_DEFAULT_MAX_RESULTS]

        status = data.get("status", {})
        response_cursor = status.get("minCursor") or status.get("rowId")

        return QueryResult(
            columns=columns,
            rows=rows,
            cursor=response_cursor if has_more or response_cursor else None,
            has_more=has_more,
        )

    def _translate_api_error(
        self, exc: httpx.HTTPStatusError, source_name: str
    ) -> AxiomAPIError:
        status = exc.response.status_code
        logger.error(
            "Axiom API error: source={source} status={status}",
            source=source_name,
            status=status,
        )
        if status in (401, 403):
            msg = f"Authentication failed for Axiom source '{source_name}'"
        elif status == 404:
            msg = f"Resource not found on Axiom source '{source_name}'"
        else:
            msg = f"Axiom query failed on source '{source_name}' (status {status})"
        return AxiomAPIError(msg, status_code=status)

    def _translate_request_error(
        self, exc: httpx.RequestError, source_name: str
    ) -> AxiomConnectionError:
        error_type = type(exc).__name__
        logger.error(
            "Axiom connection error: source={source} type={error_type}",
            source=source_name,
            error_type=error_type,
        )
        if isinstance(exc, httpx.ReadTimeout):
            msg = f"Axiom query timed out on source '{source_name}'"
        elif isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout)):
            msg = f"Failed to connect to Axiom source '{source_name}'"
        else:
            msg = f"Connection error with Axiom source '{source_name}'"
        return AxiomConnectionError(msg)
