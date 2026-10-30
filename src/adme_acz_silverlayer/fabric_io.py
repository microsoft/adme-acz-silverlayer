"""Fabric and OneLake boundary helpers for local development tests."""

from __future__ import annotations

import os
import re
import uuid
from collections.abc import Callable
from typing import Any

DEFAULT_LAKEHOUSE_NAME = "osducatalog"


def requires_path_fallback(exc: Exception) -> bool:
    msg = str(exc)
    return (
        "No default context found" in msg
        or "partial namespaces" in msg
        or "attach a lakehouse" in msg.lower()
    )


def _lakehouse_default_schema(metadata: object, workspace_id: str, lakehouse_id: str) -> str | None:
    if not isinstance(metadata, dict) or metadata.get("type") != "Lakehouse":
        raise ValueError("Get Lakehouse returned invalid lakehouse metadata.")
    for field, expected in (("workspaceId", workspace_id), ("id", lakehouse_id)):
        actual = metadata.get(field)
        try:
            matches = isinstance(actual, str) and uuid.UUID(actual) == uuid.UUID(expected)
        except (ValueError, AttributeError):
            matches = False
        if not matches:
            raise ValueError(f"Get Lakehouse returned a mismatched {field}.")
    properties = metadata.get("properties")
    if not isinstance(properties, dict):
        raise ValueError("Get Lakehouse returned invalid properties.")
    if "defaultSchema" not in properties:
        return None
    schema = properties["defaultSchema"]
    if not isinstance(schema, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("Get Lakehouse returned an invalid defaultSchema.")
    return schema


def _lakehouse_table_path(table_name: str, default_schema: str | None) -> str:
    """Resolve bare or schema-qualified Spark names to a Tables-relative path."""
    if not isinstance(table_name, str):
        raise ValueError("OneLake table name must be a string.")
    parts = table_name.split(".")
    if len(parts) not in (1, 2) or any(
        not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in parts
    ):
        raise ValueError("OneLake fallback requires a bare table name or schema.table.")
    if default_schema is not None and (
        not isinstance(default_schema, str)
        or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", default_schema)
    ):
        raise ValueError("OneLake default schema is invalid.")
    if len(parts) == 2:
        if default_schema is None:
            raise ValueError("A schema-qualified table requires a schema-enabled lakehouse.")
        return "/".join(parts)
    return f"{default_schema}/{parts[0]}" if default_schema is not None else parts[0]


def onelake_table_uri(
    workspace_id: str, lakehouse_id: str, table_name: str,
    default_schema: str | None = None,
    endpoint: str = "onelake.dfs.fabric.microsoft.com",
) -> str:
    lakehouse_ref = lakehouse_id or os.environ.get("ADME_LAKEHOUSE_NAME", DEFAULT_LAKEHOUSE_NAME)
    path = _lakehouse_table_path(table_name, default_schema)
    return f"abfss://{workspace_id}@{endpoint}/{lakehouse_ref}/Tables/{path}"


def table_path_uri(
    table_name: str,
    workspace_id: str | None,
    lakehouse_id: str | None,
    uri_builder: Callable[[str, str, str], str] = onelake_table_uri,
) -> str:
    if not workspace_id or not lakehouse_id:
        raise ValueError(
            "workspace_id/lakehouse_id not resolved; run Configuration cell before pipeline execution."
        )
    return uri_builder(workspace_id, lakehouse_id, table_name)


def table_uri(workspace_id: str, lakehouse_id: str, table_name: str) -> str:
    return table_name


def fabric_storage_options(credential: Any, storage_scope: str) -> dict[str, str]:
    token = credential.get_token(storage_scope).token
    return {
        "bearer_token": token,
        "use_fabric_endpoint": "true",
    }


def metadata_table_name(workspace_id: str, lakehouse_id: str, table_name: str) -> str:
    return table_uri(workspace_id, lakehouse_id, table_name)


def metadata_table_names(
    workspace_id: str,
    lakehouse_id: str,
    *,
    run_manifest_table: str = "silver_run_manifest",
    run_status_table: str = "silver_run_status",
    schema_cache_table: str = "silver_schema_cache",
    output_docs_table: str = "silver_output_documentation",
    data_quality_issues_table: str = "silver_data_quality_issues",
    incremental_state_table: str = "silver_incremental_state",
) -> list[str]:
    return [
        metadata_table_name(workspace_id, lakehouse_id, "silver_run_info"),
        metadata_table_name(workspace_id, lakehouse_id, run_manifest_table),
        metadata_table_name(workspace_id, lakehouse_id, run_status_table),
        metadata_table_name(workspace_id, lakehouse_id, schema_cache_table),
        metadata_table_name(workspace_id, lakehouse_id, output_docs_table),
        metadata_table_name(workspace_id, lakehouse_id, data_quality_issues_table),
        metadata_table_name(workspace_id, lakehouse_id, incremental_state_table),
        "silver_writer_guard",
        "silver_publication_attempts",
        "silver_output_ownership",
    ]
