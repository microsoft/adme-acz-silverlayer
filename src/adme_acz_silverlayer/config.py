"""Configuration parsing helpers shared by the notebook and tests."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence

DEFAULT_MERGE_KEY_COLUMNS = ["id", "version"]
DEFAULT_RETRY_STATUS_CODES = [408, 429, 500, 502, 503, 504]


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
