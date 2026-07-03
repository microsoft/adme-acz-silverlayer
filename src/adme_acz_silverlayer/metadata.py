"""Pure metadata helpers shared by notebook orchestration and tests."""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from typing import Any


def timings_json(timings: dict[str, float]) -> str:
    return json.dumps({name: round(float(elapsed), 3) for name, elapsed in sorted(timings.items())}, sort_keys=True)


def output_tables_from_results(results: Iterable[Any], fallback_tables: Sequence[str] | None = None) -> list[str]:
    table_names: list[str] = []
    for result in results:
        parent_table = getattr(result, "parent_table", None)
        child_tables = getattr(result, "child_tables", None)
        if parent_table:
            table_names.append(parent_table)
        table_names.extend(child_tables or [])
    if not table_names and fallback_tables:
        table_names.extend(fallback_tables)
    return list(dict.fromkeys(table_names))


def data_quality_enabled(value: bool | None = None) -> bool:
    return True if value is None else bool(value)


def data_quality_max_examples(value: str | int | None = None) -> int:
    raw_value = 100 if value in (None, "") else value
    return max(1, int(raw_value))


def data_quality_issues_table_name(value: str | None = None) -> str:
    return value or "silver_data_quality_issues"
