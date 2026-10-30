"""Configuration parsing helpers shared by the notebook and tests."""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping, Sequence

DEFAULT_MERGE_KEY_COLUMNS = ["id", "version"]
DEFAULT_RETRY_STATUS_CODES = [408, 429, 500, 502, 503, 504]

# Types describe the public parameter cell, not a second set of defaults.
NOTEBOOK_PARAMETER_TYPES = {
    **dict.fromkeys((
        "WORKSPACE_ID", "LAKEHOUSE_ID", "BRONZE_TABLE", "ADME_ENDPOINT",
        "ADME_DATA_PARTITION_ID", "ADME_AUTH_METHOD", "ADME_TENANT_ID",
        "ADME_SP_CLIENT_ID", "ADME_SP_SECRET_KV_NAME", "ADME_SP_SECRET_NAME",
        "ADME_MANAGED_IDENTITY_CLIENT_ID", "RUN_PROFILE", "OUTPUT_MODE",
        "TABLE_PREFIX", "WRITE_MODE", "INCREMENTAL_WATERMARK_COLUMN",
        "INCREMENTAL_WATERMARK_MODE", "INCREMENTAL_STATE_TABLE", "VERSION_STRATEGY",
        "MISSING_SCHEMA_MODE", "SCHEMA_CACHE_TABLE", "RUN_MANIFEST_TABLE",
        "RUN_STATUS_TABLE", "OUTPUT_DOCS_MODE", "OUTPUT_DOCS_TABLE",
        "DATA_QUALITY_ISSUES_TABLE", "BRONZE_CACHE_STORAGE_LEVEL",
    ), "string"),
    **dict.fromkeys((
        "WRITE_RELATIONSHIP_BRIDGES", "ALLOW_OVERWRITE", "INCLUDE_INACTIVE_RECORDS",
        "RETRY_SKIPPED_SCHEMA_RECORDS", "CREATE_EMPTY_CHILD_TABLES", "DROP_WKT",
        "PERSIST_SCHEMA_CACHE", "WRITE_OUTPUT_DOCS", "DATA_QUALITY_CHECKS",
        "CACHE_BRONZE", "PREFLIGHT_KIND_COUNTS", "BATCH_METADATA_WRITES", "SCHEMA_PREFLIGHT",
    ), "boolean"),
    **dict.fromkeys(("LIMIT", "ADME_SCHEMA_RETRY_TOTAL", "DATA_QUALITY_MAX_EXAMPLES"), "nonnegative_integer"),
    **dict.fromkeys((
        "METADATA_FLUSH_INTERVAL", "SCHEMA_FETCH_PARALLELISM",
        "WIDE_MAX_CARDINALITY_CAP", "ADME_SCHEMA_TIMEOUT_SECONDS",
    ), "positive_integer"),
    **dict.fromkeys(("KINDS", "EXCLUDED_KINDS", "MERGE_KEY_COLUMNS"), "string_list"),
    "KIND_LIMITS": "kind_limits",
    "ADME_SCHEMA_RETRY_STATUS_CODES": "status_codes",
    "ADME_SCHEMA_RETRY_BACKOFF_SECONDS": "nonnegative_number",
    "OUTPUT_WRITE_PARALLELISM": "parallelism",
}


def _parameter_bool(value: object, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y"}:
            return True
        if text in {"0", "false", "no", "n"}:
            return False
    raise ValueError(f"{name} must be a boolean or a true/false string.")


def _parameter_int(value: object, name: str, minimum: int) -> int:
    if isinstance(value, str):
        text = value.strip()
        if text and text.isascii() and text.isdecimal():
            value = int(text)
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer greater than or equal to {minimum}.")
    return value


def _parameter_value(value: object, name: str, kind: str) -> object:
    if kind == "string":
        if not isinstance(value, str):
            raise ValueError(f"{name} must be a string.")
        if name in {
            "BRONZE_TABLE", "ADME_AUTH_METHOD", "RUN_PROFILE", "OUTPUT_MODE", "WRITE_MODE",
            "INCREMENTAL_WATERMARK_MODE", "INCREMENTAL_STATE_TABLE", "VERSION_STRATEGY",
            "MISSING_SCHEMA_MODE", "SCHEMA_CACHE_TABLE", "RUN_MANIFEST_TABLE", "RUN_STATUS_TABLE",
            "OUTPUT_DOCS_MODE", "OUTPUT_DOCS_TABLE", "DATA_QUALITY_ISSUES_TABLE", "BRONZE_CACHE_STORAGE_LEVEL",
        } and not value.strip():
            raise ValueError(f"{name} must not be empty.")
        return value
    if kind == "boolean":
        return _parameter_bool(value, name)
    if kind in {"positive_integer", "nonnegative_integer"}:
        return _parameter_int(value, name, 1 if kind == "positive_integer" else 0)
    if kind == "parallelism":
        if isinstance(value, str) and value.strip().lower() == "auto":
            return "auto"
        return _parameter_int(value, name, 1)
    if kind == "nonnegative_number":
        if type(value) not in (int, float, str):
            raise ValueError(f"{name} must be a finite nonnegative number.")
        try:
            value = float(value)
        except (ValueError, OverflowError):
            raise ValueError(f"{name} must be a finite nonnegative number.") from None
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{name} must be a finite nonnegative number.")
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.startswith(("[", "{")):
            try:
                value = json.loads(text)
            except ValueError:
                raise ValueError(f"{name} contains invalid JSON.") from None
        elif kind == "kind_limits":
            pairs = [part.strip() for part in text.replace(";", ",").split(",") if part.strip()]
            if any("=" not in pair for pair in pairs):
                raise ValueError(f"{name} must be a JSON object or kind=limit pairs.")
            value = dict(pair.split("=", 1) for pair in pairs)
        else:
            value = [part.strip() for part in text.split(",") if part.strip()]
    if kind == "kind_limits":
        if not isinstance(value, dict) or any(not isinstance(key, str) or not key.strip() for key in value):
            raise ValueError(f"{name} must be an object with nonempty string keys.")
        return {key.strip(): _parameter_int(limit, name, 0) for key, limit in value.items()}
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{name} must be a list or a JSON-array string.")
    if kind == "status_codes":
        codes = sorted({_parameter_int(code, name, 100) for code in value})
        if any(code > 599 for code in codes):
            raise ValueError(f"{name} must contain HTTP status codes between 100 and 599.")
        return codes
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise ValueError(f"{name} must contain nonempty strings.")
    result = list(dict.fromkeys(item.strip() for item in value))
    if name in {"KINDS", "MERGE_KEY_COLUMNS"} and not result:
        raise ValueError(f"{name} must contain at least one entry.")
    return result


def resolve_notebook_parameters(settings: dict, environment: dict) -> dict:
    """Resolve environment > injected values > cell defaults without truthiness fallback."""
    unknown = settings.keys() - NOTEBOOK_PARAMETER_TYPES.keys()
    missing = NOTEBOOK_PARAMETER_TYPES.keys() - settings.keys()
    if unknown or missing:
        raise ValueError(f"Notebook parameter names mismatch: missing={sorted(missing)}, unknown={sorted(unknown)}.")
    for retired, replacement in (
        ("ADME_INCREMENTAL", "ADME_WRITE_MODE"),
        ("ADME_REASSEMBLE", "ADME_OUTPUT_MODE"),
    ):
        if retired in environment:
            raise ValueError(f"{retired} is no longer supported; unset it and use {replacement}.")
    resolved = {}
    for name, kind in NOTEBOOK_PARAMETER_TYPES.items():
        env_name = name if name.startswith("ADME_") else f"ADME_{name}"
        value = environment[env_name] if env_name in environment else settings[name]
        resolved[name] = _parameter_value(value, name, kind)
        if (
            name in {"WORKSPACE_ID", "LAKEHOUSE_ID", "ADME_MANAGED_IDENTITY_CLIENT_ID"}
            and env_name in environment
            and not resolved[name].strip()
            and isinstance(settings[name], str)
            and settings[name].strip()
        ):
            raise ValueError(f"{env_name} must not be blank when {name} is configured; unset the environment override.")
    return resolved


def normalize_output_mode(value: str | None) -> str:
    normalized = (value or "normalized").strip().lower().replace("-", "_")
    aliases = {
        "normalized": "normalized",
        "normalised": "normalized",
        "parent_child": "normalized",
        "parent_children": "normalized",
        "parent+children": "normalized",
        "wide": "wide",
        "flat": "wide",
        "reassembled": "wide",
        "reassemble": "wide",
    }
    if normalized not in aliases:
        raise ValueError("OUTPUT_MODE must be 'normalized' or 'wide'.")
    return aliases[normalized]


def normalize_version_strategy(value: str | None) -> str:
    normalized = (value or "merge").strip().lower().replace("-", "_")
    if normalized not in {"merge", "versioned_tables"}:
        raise ValueError("VERSION_STRATEGY must be 'merge' or 'versioned_tables'.")
    return normalized


def normalize_missing_schema_mode(value: str | None) -> str:
    normalized = (value or "skip").strip().lower().replace("-", "_")
    if normalized not in {"skip", "infer", "fail"}:
        raise ValueError("MISSING_SCHEMA_MODE must be 'skip', 'infer', or 'fail'.")
    return normalized


def normalize_output_docs_mode(value: str | None) -> str:
    normalized = (value or "summary").strip().lower().replace("-", "_")
    if normalized not in {"off", "summary", "full"}:
        raise ValueError("OUTPUT_DOCS_MODE must be 'off', 'summary', or 'full'.")
    return normalized


def normalize_write_mode(value: str | None, incremental_flag: bool) -> str:
    normalized = (value or "").strip().lower().replace("-", "_")
    if not normalized:
        return "upsert" if incremental_flag else "full_refresh"
    aliases = {
        "full": "full_refresh",
        "full_refresh": "full_refresh",
        "overwrite": "full_refresh",
        "upsert": "upsert",
        "incremental": "upsert",
        "merge": "upsert",
    }
    if normalized not in aliases:
        raise ValueError("WRITE_MODE must be 'full_refresh' or 'upsert'.")
    return aliases[normalized]


def normalize_watermark_mode(value: str | None) -> str:
    normalized = (value or "auto").strip().lower().replace("-", "_")
    if normalized not in {"off", "auto", "required"}:
        raise ValueError("INCREMENTAL_WATERMARK_MODE must be 'off', 'auto', or 'required'.")
    return normalized


def parse_retry_status_codes(value: str | Sequence[int] | None) -> list[int]:
    raw_values: str | Sequence[int] = DEFAULT_RETRY_STATUS_CODES if value in (None, "") else value
    if isinstance(raw_values, str):
        if raw_values.strip().startswith("["):
            loaded = json.loads(raw_values)
            if not isinstance(loaded, list):
                raise ValueError("ADME_SCHEMA_RETRY_STATUS_CODES JSON must be a list.")
            raw_values = loaded
        else:
            raw_values = [part.strip() for part in raw_values.split(",") if part.strip()]
    parsed = sorted({int(code) for code in raw_values})
    if any(code < 100 or code > 599 for code in parsed):
        raise ValueError("ADME_SCHEMA_RETRY_STATUS_CODES must contain HTTP status codes.")
    return parsed


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


def parse_kind_limits(value: str | Mapping[str, int] | None) -> dict[str, int]:
    if value in (None, ""):
        return {}

    if isinstance(value, Mapping):
        raw_items = value.items()
    else:
        text = str(value).strip()
        if not text:
            return {}
        if text.startswith("{"):
            loaded = json.loads(text)
            if not isinstance(loaded, dict):
                raise ValueError("ADME_KIND_LIMITS JSON must be an object.")
            raw_items = loaded.items()
        else:
            pairs = [part.strip() for part in text.replace(";", ",").split(",") if part.strip()]
            parsed_pairs: list[tuple[str, str]] = []
            for pair in pairs:
                if "=" not in pair:
                    raise ValueError("ADME_KIND_LIMITS entries must use key=value format.")
                key, raw_limit = pair.split("=", 1)
                parsed_pairs.append((key.strip(), raw_limit.strip()))
            raw_items = parsed_pairs

    parsed: dict[str, int] = {}
    for key, raw_limit in raw_items:
        clean_key = str(key).strip()
        if not clean_key:
            raise ValueError("KIND_LIMITS contains an empty kind key.")
        limit_value = int(raw_limit)
        if limit_value < 0:
            raise ValueError("KIND_LIMITS values must be greater than or equal to 0.")
        parsed[clean_key] = limit_value
    return parsed


def parse_merge_key_columns(value: str | Sequence[str] | None) -> list[str]:
    if value in (None, ""):
        raw_values: Sequence[str] = DEFAULT_MERGE_KEY_COLUMNS
    elif isinstance(value, Sequence) and not isinstance(value, str):
        raw_values = value
    else:
        text = str(value).strip()
        if text.startswith("["):
            loaded = json.loads(text)
            if not isinstance(loaded, list):
                raise ValueError("ADME_MERGE_KEY_COLUMNS JSON must be a list.")
            raw_values = loaded
        else:
            raw_values = [part.strip() for part in text.split(",")]

    parsed = [str(col).strip() for col in raw_values if str(col).strip()]
    if not parsed:
        raise ValueError("MERGE_KEY_COLUMNS must contain at least one column.")
    return list(dict.fromkeys(parsed))


def bounded_positive_int(value: str | int | None, default: int, name: str) -> int:
    raw_value = default if value in (None, "") else value
    parsed = int(raw_value)
    if parsed < 1:
        raise ValueError(f"{name} must be greater than or equal to 1.")
    return parsed
