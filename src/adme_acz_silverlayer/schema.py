"""Pure JSON Schema compatibility helpers."""

from __future__ import annotations

from typing import Any


def schema_definitions(raw: dict[str, Any]) -> dict[str, Any]:
    definitions = dict(raw.get("definitions", {}) or {})
    definitions.update(raw.get("$defs", {}) or {})
    return definitions


def definition_key_from_ref(ref: str) -> str | None:
    for prefix in ("#/definitions/", "#/$defs/"):
        if ref.startswith(prefix):
            return ref[len(prefix) :]
    return None


def first_non_null_json_type(json_type: Any) -> str | None:
    if isinstance(json_type, list):
        non_null = [str(value) for value in json_type if str(value) != "null"]
        for preferred in ("object", "array", "string", "number", "integer", "boolean"):
            if preferred in non_null:
                return preferred
        return non_null[0] if non_null else None
    return str(json_type) if json_type else None


def normalize_kind_ref(ref: str | None) -> str | None:
    if not ref:
        return None
    normalized = ref.replace("{{schema-authority}}", "osdu")
    normalized = normalized.replace("{{wksNameSpace}}", "wks")
    normalized = normalized.replace("{{wksVersion}}", "1.0.0")
    return normalized


def assign_nested_property(properties: dict[str, Any], path_parts: list[str], schema: dict[str, Any]) -> None:
    clean_parts = [part for part in path_parts if part]
    if not clean_parts:
        return
    head = clean_parts[0]
    if len(clean_parts) == 1:
        properties[head] = schema
        return
    node = properties.setdefault(head, {"type": "object", "properties": {}})
    if not isinstance(node, dict) or node.get("type") != "object":
        node = {"type": "object", "properties": {}}
        properties[head] = node
    node.setdefault("properties", {})
    assign_nested_property(node["properties"], clean_parts[1:], schema)
