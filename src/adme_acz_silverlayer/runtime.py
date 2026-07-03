"""Runtime option helpers shared by notebook orchestration and tests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from .config import DEFAULT_MERGE_KEY_COLUMNS


def effective_merge_key_columns(
    merge_key_columns: Sequence[str] | None = None,
    default_columns: Sequence[str] = DEFAULT_MERGE_KEY_COLUMNS,
) -> list[str]:
    columns = merge_key_columns or default_columns
    parsed = [str(col).strip() for col in columns if str(col).strip()]
    if not parsed:
        raise ValueError("At least one merge key column is required.")
    return parsed


def merge_condition(target_alias: str, source_alias: str, merge_key_columns: Sequence[str]) -> str:
    return " AND ".join(f"{target_alias}.`{col}` = {source_alias}.`{col}`" for col in merge_key_columns)


def bounded_runtime_int(value: str | int | None, default: int) -> int:
    raw_value = default if value in (None, "") else value
    return max(1, int(raw_value))


def schema_fetch_parallelism(value: str | int | None = None) -> int:
    return bounded_runtime_int(value, 4)


def wide_max_cardinality_cap(value: str | int | None = None) -> int:
    return bounded_runtime_int(value, 20)


def watermark_active(incremental: bool, watermark_column: str | None, watermark_mode: str | None) -> bool:
    return bool(incremental and watermark_column and (watermark_mode or "auto") != "off")


def validate_incremental_limit_safety(
    incremental: bool,
    watermark_column: str | None,
    watermark_mode: str | None,
    limit: int | None,
    kind_limits: Mapping[str, int] | None,
) -> None:
    if not watermark_active(incremental, watermark_column, watermark_mode):
        return
    limited_kinds = {key: value for key, value in (kind_limits or {}).items() if value}
    if limit or limited_kinds:
        raise ValueError(
            "Watermark-based upsert cannot run with LIMIT or KIND_LIMITS because advancing the watermark after a limited batch can skip unprocessed rows. "
            "Remove ADME_LIMIT/ADME_KIND_LIMITS for scheduled incremental runs, or set INCREMENTAL_WATERMARK_MODE='off'."
        )


def retry_skipped_schema_records(value: bool | None = None) -> bool:
    return True if value is None else bool(value)
