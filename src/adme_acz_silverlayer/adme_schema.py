"""ADME auth and schema-service boundary helpers for local development tests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from urllib.parse import quote

ADME_SCHEMA_SERVICE_PATH = "/api/schema-service/v1/schema"
ADME_TOKEN_SCOPE = "https://management.core.windows.net/.default"
DEFAULT_RETRY_STATUS_CODES = [408, 429, 500, 502, 503, 504]


def adme_auth_method(method: str | None = None, fallback_method: str | None = None) -> str:
    normalized = str(method or fallback_method or "SP").strip().upper()
    if normalized not in {"SP", "DC", "MI"}:
        raise ValueError("ADME_AUTH_METHOD must be 'SP', 'DC', or 'MI'.")
    return normalized


def adme_auth_value(
    name: str,
    values: Mapping[str, object],
    *,
    required: bool = True,
    method: str | None = None,
) -> str:
    lower_name = name.lower()
    value = str(values.get(lower_name) or values.get(name) or "").strip()
    if required and not value:
        auth_method = adme_auth_method(method or str(values.get("adme_auth_method") or values.get("ADME_AUTH_METHOD") or "SP"))
        raise ValueError(f"{name} must be configured for ADME { auth_method } authentication.")
    return value


def adme_authority_url(tenant_id: str) -> str:
    return f"https://login.microsoftonline.com/{tenant_id}"


def adme_managed_identity_client_id(client_id: str | None = None) -> str:
    return str(client_id or "").strip()


def adme_keyvault_url(kv_name_or_url: str) -> str:
    value = kv_name_or_url.strip()
    if value.lower().startswith("https://"):
        return value.rstrip("/") + "/"
    return f"https://{value}.vault.azure.net/"


def schema_doc_cache_key(kind: str, endpoint: str, data_partition_id: str) -> tuple[str, str, str]:
    return endpoint, data_partition_id, kind


def adme_schema_url(kind: str, endpoint: str, path: str = ADME_SCHEMA_SERVICE_PATH) -> str:
    return f"{endpoint}{path}/{quote(kind, safe=':.-_')}"


def adme_schema_source_prefix(endpoint: str, data_partition_id: str) -> str:
    return f"adme:endpoint={endpoint};partition={data_partition_id};"


def adme_schema_source(schema_url: str, endpoint: str, data_partition_id: str) -> str:
    return f"{adme_schema_source_prefix(endpoint, data_partition_id)}url={schema_url}"


def adme_schema_list_url(endpoint: str, limit: int = 1, path: str = ADME_SCHEMA_SERVICE_PATH) -> str:
    safe_limit = max(1, int(limit))
    return f"{endpoint}{path}?latestVersion=False&limit={safe_limit}"


def adme_response_detail(response) -> str:
    detail = (getattr(response, "text", "") or "").strip().replace("\n", " ")
    return f"{detail[:1000]}..." if len(detail) > 1000 else detail


def adme_schema_timeout(timeout: int | None = None, default: int = 30) -> int:
    return int(timeout or default)


def adme_schema_retry_status_codes(codes: Sequence[int] | None = None) -> list[int]:
    raw_codes = DEFAULT_RETRY_STATUS_CODES if codes is None else codes
    return [int(code) for code in raw_codes]


def adme_schema_retry_summary(
    total: int = 3,
    backoff_seconds: float = 1.0,
    status_codes: Sequence[int] | None = None,
) -> str:
    return (
        f"total={int(total)}, "
        f"backoff={float(backoff_seconds)}, "
        f"statuses={adme_schema_retry_status_codes(status_codes)}"
    )
