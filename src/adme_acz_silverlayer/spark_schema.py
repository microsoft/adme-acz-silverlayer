"""Existing notebook JSON Schema to Spark conversion, without a Spark session.

This extraction preserves the notebook's compatibility behavior, including
string fallbacks and first-supported-branch selection for alternatives.
"""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import types as T

from .schema import (
    definition_key_from_ref as _definition_key_from_ref,
    first_non_null_json_type as _first_non_null_json_type,
    schema_definitions as _schema_definitions,
)

_JSON_TYPE_MAP: dict[str, T.DataType] = {
    "string": T.StringType(),
    "integer": T.LongType(),
    "number": T.DoubleType(),
    "boolean": T.BooleanType(),
}


def _with_struct_field_type(field: T.StructField, data_type: T.DataType) -> T.StructField:
    return T.StructField(field.name, data_type, nullable=field.nullable, metadata=field.metadata)


def _merge_struct_fields(existing: T.StructField, incoming: T.StructField) -> T.StructField:
    if isinstance(existing.dataType, T.NullType):
        return incoming
    if isinstance(incoming.dataType, T.NullType):
        return existing
    merged_type = _merge_schemas(existing.dataType, incoming.dataType)
    return T.StructField(
        existing.name,
        merged_type,
        nullable=existing.nullable or incoming.nullable,
        metadata=existing.metadata,
    )


def _dedupe_case_insensitive_struct_type(data_type: T.DataType) -> T.DataType:
    if isinstance(data_type, T.StructType):
        fields_by_key: dict[str, T.StructField] = {}
        ordered_keys: list[str] = []
        for field in data_type.fields:
            nested_type = _dedupe_case_insensitive_struct_type(field.dataType)
            normalized_field = _with_struct_field_type(field, nested_type)
            key = field.name.casefold()
            if key in fields_by_key:
                fields_by_key[key] = _merge_struct_fields(fields_by_key[key], normalized_field)
            else:
                fields_by_key[key] = normalized_field
                ordered_keys.append(key)
        return T.StructType([fields_by_key[key] for key in ordered_keys])
    if isinstance(data_type, T.ArrayType):
        element_type = _dedupe_case_insensitive_struct_type(data_type.elementType)
        return T.ArrayType(element_type, containsNull=data_type.containsNull)
    if isinstance(data_type, T.MapType):
        value_type = _dedupe_case_insensitive_struct_type(data_type.valueType)
        return T.MapType(data_type.keyType, value_type, valueContainsNull=data_type.valueContainsNull)
    return data_type


def _merge_struct_types(a: T.StructType, b: T.StructType) -> T.StructType:
    fields_by_key: dict[str, T.StructField] = {}
    ordered_keys: list[str] = []
    for field in [*a.fields, *b.fields]:
        normalized_type = _dedupe_case_insensitive_struct_type(field.dataType)
        normalized_field = _with_struct_field_type(field, normalized_type)
        key = field.name.casefold()
        if key in fields_by_key:
            fields_by_key[key] = _merge_struct_fields(fields_by_key[key], normalized_field)
        else:
            fields_by_key[key] = normalized_field
            ordered_keys.append(key)
    return T.StructType([fields_by_key[key] for key in ordered_keys])


def _merge_schemas(a: T.DataType, b: T.DataType) -> T.DataType:
    if isinstance(a, T.StructType) and isinstance(b, T.StructType):
        return _merge_struct_types(a, b)
    if isinstance(a, T.ArrayType) and isinstance(b, T.ArrayType):
        merged_elem = _merge_schemas(a.elementType, b.elementType)
        return _dedupe_case_insensitive_struct_type(T.ArrayType(merged_elem, containsNull=True))
    if isinstance(a, T.NullType):
        return _dedupe_case_insensitive_struct_type(b)
    return _dedupe_case_insensitive_struct_type(a)


def _resolve_node(
    node: dict[str, Any],
    definitions: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(node, dict):
        return node

    if "$ref" in node and definitions:
        ref_key = _definition_key_from_ref(node["$ref"])
        resolved = definitions.get(ref_key) if ref_key else None
        if resolved:
            return _resolve_node(resolved, definitions)

    if "allOf" in node:
        merged: dict[str, Any] = {}
        for sub in node["allOf"]:
            if not isinstance(sub, dict):
                continue
            resolved = _resolve_node(sub, definitions)
            if "properties" in resolved:
                merged.update(resolved["properties"])
        if merged:
            return {"type": "object", "properties": merged}

    return node


def _json_schema_to_spark(
    schema_node: dict[str, Any],
    definitions: dict[str, Any] | None = None,
) -> T.DataType:
    if not isinstance(schema_node, dict):
        return T.StringType()

    json_type = _first_non_null_json_type(schema_node.get("type"))

    if "$ref" in schema_node and not json_type:
        if definitions:
            ref_key = _definition_key_from_ref(schema_node["$ref"])
            resolved = definitions.get(ref_key) if ref_key else None
            if resolved:
                return _json_schema_to_spark(resolved, definitions)
        return T.StringType()

    if "allOf" in schema_node and not json_type:
        merged_props: dict[str, Any] = {}
        for sub in schema_node["allOf"]:
            if not isinstance(sub, dict):
                continue
            resolved = sub
            if "$ref" in sub and definitions:
                ref_key = _definition_key_from_ref(sub["$ref"])
                r = definitions.get(ref_key) if ref_key else None
                if r:
                    resolved = r
            if "properties" in resolved:
                merged_props.update(resolved["properties"])
        if merged_props:
            fields = []
            for name, prop_schema in merged_props.items():
                spark_type = _json_schema_to_spark(prop_schema, definitions)
                fields.append(T.StructField(name, spark_type, nullable=True))
            return _dedupe_case_insensitive_struct_type(T.StructType(fields))
        for sub in schema_node["allOf"]:
            if isinstance(sub, dict) and ("type" in sub or "properties" in sub):
                return _json_schema_to_spark(sub, definitions)
        return T.StringType()

    for composite_key in ("anyOf", "oneOf"):
        if composite_key in schema_node and not json_type:
            for sub in schema_node[composite_key]:
                if not isinstance(sub, dict):
                    continue
                sub_type = _first_non_null_json_type(sub.get("type"))
                if sub_type == "null":
                    continue
                if "$ref" in sub or sub_type or "properties" in sub or "items" in sub:
                    return _json_schema_to_spark(sub, definitions)
            return T.StringType()

    if json_type == "object":
        props = schema_node.get("properties", {})
        if props:
            fields = []
            for name, prop_schema in props.items():
                spark_type = _json_schema_to_spark(prop_schema, definitions)
                fields.append(T.StructField(name, spark_type, nullable=True))
            return _dedupe_case_insensitive_struct_type(T.StructType(fields))
        return T.MapType(T.StringType(), T.StringType())

    if json_type == "array":
        items = schema_node.get("items", {})
        element_type = _json_schema_to_spark(items, definitions)
        return _dedupe_case_insensitive_struct_type(T.ArrayType(element_type, containsNull=True))

    if json_type in _JSON_TYPE_MAP:
        return _JSON_TYPE_MAP[json_type]

    fmt = schema_node.get("format", "")
    if fmt in ("date-time", "date"):
        return T.StringType()

    return T.StringType()


def _classify_spark_type(dt: T.DataType) -> str:
    if isinstance(dt, T.ArrayType):
        return "json_array"
    if isinstance(dt, (T.StructType, T.MapType)):
        return "json_object"
    return "scalar"


def _parse_osdu_schema(schema_json: str | dict) -> dict[str, Any]:
    raw = json.loads(schema_json) if isinstance(schema_json, str) else schema_json

    definitions = _schema_definitions(raw)
    top_props = dict(raw.get("properties", {}))

    if "allOf" in raw:
        for sub in raw["allOf"]:
            if not isinstance(sub, dict):
                continue
            if "properties" in sub:
                top_props.update(sub["properties"])
            elif "$ref" in sub:
                def_key = _definition_key_from_ref(sub["$ref"])
                defn = definitions.get(def_key, {}) if def_key else {}
                def_props = defn.get("properties", {})
                top_props.update(def_props)

    envelope_fields = {k: v for k, v in top_props.items() if k != "data"}
    data_node = top_props.get("data", {})
    data_fields = dict(data_node.get("properties", {}))

    if "allOf" in data_node:
        for sub in data_node["allOf"]:
            if not isinstance(sub, dict):
                continue
            if "properties" in sub:
                data_fields.update(sub["properties"])
            elif "$ref" in sub:
                def_key = _definition_key_from_ref(sub["$ref"])
                defn = definitions.get(def_key, {}) if def_key else {}
                def_props = defn.get("properties", {})
                data_fields.update(def_props)

    return {
        "envelope_fields": envelope_fields,
        "data_fields": data_fields,
        "definitions": definitions,
        "raw": raw,
    }


# Public entry points share the exact implementation embedded in the notebook.
json_schema_to_spark = _json_schema_to_spark
parse_osdu_schema = _parse_osdu_schema
dedupe_struct_type = _dedupe_case_insensitive_struct_type
