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


def output_write_parallelism_for_sku(sku_size: str | None) -> int:
    normalized = str(sku_size or "").strip().upper()
    if not normalized:
        return 1
    if normalized in {"TRIAL", "FTL4"}:
        return 1
    if normalized.startswith("P") and normalized[1:].isdigit():
        size = int(normalized[1:])
        if size <= 1:
            return 4
        if size == 2:
            return 6
        return 8
    if normalized.startswith("F") and normalized[1:].isdigit():
        size = int(normalized[1:])
        if size < 16:
            return 1
        if size < 32:
            return 2
        if size < 64:
            return 3
        if size < 128:
            return 4
        if size < 256:
            return 6
        return 8
    return 1


def output_write_parallelism(value: str | int | None = None, sku_size: str | None = None) -> int:
    raw_value = "auto" if value in (None, "") else value
    if str(raw_value).strip().lower() == "auto":
        return output_write_parallelism_for_sku(sku_size)
    return bounded_runtime_int(raw_value, 1)


def wide_max_cardinality_cap(value: str | int | None = None) -> int:
    return bounded_runtime_int(value, 20)


def watermark_active(incremental: bool, watermark_column: str | None, watermark_mode: str | None) -> bool:
    return bool(incremental and watermark_column and (watermark_mode or "auto") != "off")


def processing_limits_active(limit: int | None, kind_limits: Mapping[str, int] | None) -> bool:
    return bool(limit) or any(bool(value) for value in (kind_limits or {}).values())


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
