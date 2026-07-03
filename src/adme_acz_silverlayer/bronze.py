"""Bronze-table boundary helpers for local development tests."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def include_inactive_records(value: bool | None = None) -> bool:
    return bool(False if value is None else value)


def active_record_filter_status(df_or_columns: Any, include_inactive_records_value: bool | None = None) -> tuple[bool, str]:
    include_inactive = include_inactive_records(include_inactive_records_value)
    if include_inactive:
        return True, "inactive records are included by configuration"

    columns_source = getattr(df_or_columns, "columns", df_or_columns)
    columns = set(columns_source if isinstance(columns_source, Iterable) and not isinstance(columns_source, str) else [])
    if "isActive" not in columns:
        return False, "Bronze table must contain an 'isActive' column when INCLUDE_INACTIVE_RECORDS is False."
    return True, "excluding rows where isActive is not true"
