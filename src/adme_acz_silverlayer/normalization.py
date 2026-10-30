"""Spark normalization primitives shared with the self-contained Fabric notebook."""

from __future__ import annotations

import hashlib
import re

from pyspark.sql import Column, DataFrame, functions as F, types as T


_DELTA_COLUMN_PART_RE = re.compile(r"[^0-9A-Za-z_]+")


def record_key(id_column: Column, version_column: Column) -> Column:
    """Encode exact record identity as id:version with a numeric version suffix."""
    identity = [id_column.cast("string"), version_column.cast("string")]
    invalid = identity[0].isNull() | identity[1].isNull() | (identity[0] == "") | ~identity[1].rlike(r"\A[0-9]+\z")
    return F.when(
        invalid, F.raise_error("Record identity requires nonempty id and an ASCII-digit version."),
    ).otherwise(F.concat(identity[0], F.lit(":"), identity[1]))


def _sanitize_column_name_part(value: str, fallback: str = "field") -> str:
    clean = _DELTA_COLUMN_PART_RE.sub("_", str(value or "").strip())
    clean = re.sub(r"_+", "_", clean).strip("_")
    if not clean:
        clean = fallback
    if clean[0].isdigit():
        clean = f"{fallback}_{clean}"
    return clean


def sanitize_delta_column_name(column_name: str, fallback: str = "field") -> str:
    """Normalize a flattened source path to a Delta-safe column name."""
    parts = [part for part in str(column_name or "").replace(".", "__").split("__") if part]
    clean = "__".join(_sanitize_column_name_part(part, fallback=fallback) for part in parts)
    return clean or fallback


def make_delta_column_alias(raw_name: str, used: set[str] | None = None, fallback: str = "field") -> str:
    """Allocate a deterministic alias unique under case-insensitive column resolution."""
    alias = sanitize_delta_column_name(raw_name, fallback=fallback)
    if used is None:
        return alias
    used_folded = {name.casefold() for name in used}
    if alias.casefold() not in used_folded:
        used.add(alias)
        return alias

    digest = hashlib.sha1(str(raw_name).encode("utf-8")).hexdigest()[:8]
    base = alias[:119].rstrip("_") or fallback
    candidate = f"{base}_{digest}"
    suffix = 2
    while candidate.casefold() in used_folded:
        suffix_text = f"_{suffix}"
        candidate = f"{base[: 128 - len(suffix_text)]}{suffix_text}"
        suffix += 1
    used.add(candidate)
    return candidate


def _quoted_top_level_col(column_name: str):
    escaped = str(column_name).replace("`", "``")
    return F.col(f"`{escaped}`")


def _nested_field_col(parent_name: str, field_name: str):
    parent_escaped = str(parent_name).replace("`", "``")
    escaped = str(field_name).replace("`", "``")
    return F.col(f"`{parent_escaped}`.`{escaped}`")


def _flatten_typed_structs(parent: DataFrame) -> DataFrame:
    struct_fields = [f for f in parent.schema.fields if isinstance(f.dataType, T.StructType)]
    if not struct_fields:
        return parent

    select_exprs = []
    used_aliases: set[str] = set()
    struct_names = {f.name for f in struct_fields}
    for field in parent.schema.fields:
        if field.name in struct_names:
            prefix = field.name
            for sub in field.dataType.fields:
                alias = make_delta_column_alias(f"{prefix}__{sub.name}", used_aliases)
                select_exprs.append(_nested_field_col(prefix, sub.name).alias(alias))
        else:
            alias = make_delta_column_alias(field.name, used_aliases)
            select_exprs.append(_quoted_top_level_col(field.name).alias(alias))

    return parent.select(select_exprs)


def _flatten_all_struct_columns(df: DataFrame) -> DataFrame:
    while any(isinstance(field.dataType, T.StructType) for field in df.schema.fields):
        df = _flatten_typed_structs(df)
    return df


def explode_array(
    df: DataFrame,
    column_name: str,
    key_columns: list[str],
    element_type: T.DataType,
) -> DataFrame:
    """Normalize one typed array, preserving keys, null elements and occurrence ordinals.

    Null and empty arrays produce no child rows. Struct elements expose their
    immediate fields; callers decide whether to flatten or infer nested JSON.
    """
    subset = df.select(*key_columns, _quoted_top_level_col(column_name).alias(column_name))
    exploded = subset.select(
        *key_columns,
        F.posexplode(_quoted_top_level_col(column_name)).alias("ordinal", "_item"),
    )
    if not isinstance(element_type, T.StructType):
        return exploded.select(*key_columns, "ordinal", F.col("_item").alias("value"))

    select_cols = [*key_columns, "ordinal"]
    used_aliases = set(select_cols)
    for field in element_type.fields:
        alias = make_delta_column_alias(field.name, used_aliases)
        select_cols.append(_nested_field_col("_item", field.name).alias(alias))
    return exploded.select(select_cols)


flatten_structs = _flatten_all_struct_columns
