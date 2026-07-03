"""Fabric and OneLake boundary helpers for local development tests."""

from __future__ import annotations

import os
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


def onelake_table_uri(workspace_id: str, lakehouse_id: str, table_name: str) -> str:
    lakehouse_ref = lakehouse_id or os.environ.get("ADME_LAKEHOUSE_NAME", DEFAULT_LAKEHOUSE_NAME)
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_ref}/Tables/{table_name}"


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
    ]
