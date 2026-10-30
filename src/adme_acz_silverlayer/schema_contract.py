"""Schema-driven projection and relationship rules for the reference pipeline.

This is a representation contract, not a full JSON Schema validator. Mixed-type
alternatives and unconstrained objects retain JSON text rather than guessed types.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path


def _relationship_targets(node: dict) -> list[dict] | None:
    if "x-osdu-relationship" not in node:
        return None
    targets = node["x-osdu-relationship"]
    if not isinstance(targets, list) or any(not isinstance(target, dict) for target in targets):
        raise ValueError("Relationship targets must be an array of descriptors")
    for target in targets:
        for key in ("GroupType", "EntityType"):
            if key in target and (not isinstance(target[key], str) or not target[key]):
                raise ValueError(f"Relationship descriptor {key} must be a nonempty string")
    return targets


def _has_relationships(plan: dict) -> bool:
    return (
        plan.get("targets") is not None
        or any(_has_relationships(child) for child in plan.get("properties", {}).values())
        or "items" in plan and _has_relationships(plan["items"])
        or any(_has_relationships(variant) for variant in plan.get("variants", []))
    )


def _merge_plans(plans: list[dict], alternative: bool) -> dict:
    if not plans:
        return {"type": "json", "targets": None, "pattern": None}
    if not alternative and any(plan["type"] != "json" for plan in plans):
        plans = [plan for plan in plans if plan["type"] != "json" or plan.get("variants") or plan.get("targets") is not None]
    types = {plan["type"] for plan in plans}
    if len(types) > 1:
        if not alternative:
            raise ValueError("Conflicting allOf types cannot be projected")
        result = {"type": "json", "variants": plans}
    else:
        result = {"type": plans[0]["type"]}
        if result["type"] == "object":
            names = dict.fromkeys(name for plan in plans for name in plan.get("properties", {}))
            if len({name.casefold() for name in names}) != len(names):
                raise ValueError("Case-insensitive composed schema property collisions")
            result["properties"] = {
                name: _merge_plans(
                    [plan["properties"][name] for plan in plans if name in plan.get("properties", {})],
                    alternative,
                )
                for name in names
            }
        elif result["type"] == "array":
            result["items"] = _merge_plans([plan["items"] for plan in plans], alternative)
        elif any("variants" in plan for plan in plans):
            result["variants"] = [variant for plan in plans for variant in plan.get("variants", [plan])]

    result.update(_merge_relationship_annotations(plans, alternative))
    return result


def _merge_relationship_annotations(plans: list[dict], alternative: bool) -> dict:
    result = {}
    declarations = [plan["targets"] for plan in plans if plan.get("targets") is not None]
    if declarations:
        if alternative:
            result["targets"] = (
                [] if any(not targets for targets in declarations)
                else list({json.dumps(target, sort_keys=True): target for targets in declarations for target in targets}.values())
            )
        else:
            intersection = [{}]
            for targets in declarations:
                if not targets:
                    continue
                intersection = [
                    {**left, **right} for left in intersection for right in targets
                    if all(not left.get(key) or not right.get(key) or left[key] == right[key]
                           for key in ("GroupType", "EntityType"))
                ]
                if not intersection:
                    raise ValueError("Contradictory inherited relationship targets")
            result["targets"] = [] if intersection == [{}] else intersection
    else:
        result["targets"] = None
    patterns = list(dict.fromkeys(plan["pattern"] for plan in plans if plan.get("pattern")))
    if alternative and any(plan.get("targets") is not None and not plan.get("pattern") for plan in plans):
        patterns = []
    result["pattern"] = (
        "|".join(f"(?:{pattern})" for pattern in patterns) if alternative and len(patterns) > 1
        else "".join(f"(?=(?:{pattern})\\Z)" for pattern in patterns) + r"[\s\S]*" if len(patterns) > 1
        else patterns[0] if patterns else None
    )
    return result


def compile_schema(schema: dict, kind: str) -> dict:
    """Resolve local references and compositions without performing network I/O."""
    if not isinstance(schema, dict) or schema.get("x-osdu-schema-source") != kind:
        raise ValueError(f"Schema identity mismatch for {kind}")

    def compile_node(node, references=()):
        if not isinstance(node, dict):
            raise ValueError("Schema nodes must be objects; tuple arrays are unsupported")
        plans = []
        if "$ref" in node:
            reference = node["$ref"]
            if not isinstance(reference, str) or not reference.startswith("#/"):
                raise ValueError("Only local JSON Schema references are supported")
            if reference in references:
                raise ValueError("Recursive schema references are unsupported")
            target = schema
            for segment in reference[2:].split("/"):
                key = segment.replace("~1", "/").replace("~0", "~")
                if not isinstance(target, dict) or key not in target:
                    raise ValueError("Unresolved local schema reference")
                target = target[key]
            plans.append(compile_node(target, references + (reference,)))

        for operator in ("allOf", "oneOf", "anyOf"):
            if operator not in node:
                continue
            branches = node[operator]
            if not isinstance(branches, list) or not branches:
                raise ValueError(f"{operator} must contain schema objects")
            meaningful = [branch for branch in branches if branch != {"type": "null"}]
            plans.append(_merge_plans(
                [compile_node(branch, references) for branch in meaningful], operator != "allOf"
            ))

        declared_type = node.get("type")
        if declared_type is not None and not isinstance(declared_type, (str, list)):
            raise ValueError("Schema type must be a string or array of strings")
        if isinstance(declared_type, list):
            if not declared_type:
                raise ValueError("Schema type arrays must not be empty")
            types = [value for value in declared_type if value != "null"]
            if any(not isinstance(value, str) for value in types):
                raise ValueError("Schema type arrays must contain strings")
            if len(types) > 1:
                plans.append(_merge_plans([
                    compile_node({**node, "type": value}, references) for value in types
                ], True))
                declared_type = None
            else:
                declared_type = types[0] if types else "null"
        if declared_type is None:
            declared_type = "object" if "properties" in node else "array" if "items" in node else None
        if declared_type is not None:
            if declared_type not in {"object", "array", "string", "integer", "number", "boolean", "null", "json"}:
                raise ValueError(f"Unsupported JSON Schema type: {declared_type}")
            plan = {"type": declared_type, "targets": None, "pattern": None}
            if declared_type == "object":
                properties = node.get("properties", {})
                if not isinstance(properties, dict):
                    raise ValueError("Schema properties must be an object")
                if len({name.casefold() for name in properties}) != len(properties):
                    raise ValueError("Case-insensitive schema property collisions are unsupported")
                if properties or any(existing["type"] == "object" for existing in plans):
                    plan["properties"] = {
                        name: compile_node(value, references) for name, value in properties.items()
                    }
                else:
                    plan["type"] = "json"
            elif declared_type == "array":
                plan["items"] = compile_node(node.get("items", {}), references)
            additional = node.get("additionalProperties")
            if isinstance(additional, dict) and _has_relationships(compile_node(additional, references)):
                raise ValueError("Relationships inside opaque additionalProperties maps are unsupported")
            plans.append(plan)

        result = _merge_plans(plans, False)
        targets = _relationship_targets(node)
        pattern = node.get("pattern")
        if pattern is not None:
            if not isinstance(pattern, str):
                raise ValueError("Schema reference patterns must be strings")
            re.compile(pattern, re.ASCII)
        result.update(_merge_relationship_annotations(
            [result, {"targets": targets, "pattern": pattern}], False
        ))
        if result["type"] == "array" and result["targets"] is not None:
            result["items"] = {
                **result["items"],
                **_merge_relationship_annotations([result["items"], result], False),
            }
            result["targets"] = None
        return result

    plan = compile_node(schema)
    if plan["type"] != "object" or "data" not in plan.get("properties", {}):
        raise ValueError(f"Schema must declare an object envelope with data: {kind}")
    return plan


def load_schema_directory(directory: str, kinds: list[str]) -> dict[str, dict]:
    """Load exact exported schemas named by replacing kind colons with underscores."""
    schemas = {}
    for kind in kinds:
        if not isinstance(kind, str) or not re.fullmatch(
            r"[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+:[0-9]+\.[0-9]+\.[0-9]+", kind
        ):
            raise ValueError("Kind cannot be represented safely as a schema filename")
        path = Path(directory) / (kind.replace(":", "_") + ".json")
        schema = parse_json(path.read_text(encoding="utf-8"))
        compile_schema(schema, kind)
        schemas[kind] = schema
    return schemas


def _json_object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("Duplicate JSON property")
        result[name] = value
    return result


def _invalid_constant(value):
    raise ValueError("Non-finite JSON numbers are unsupported")


def parse_json(value: str):
    """Parse JSON without accepting duplicate keys or non-finite constants."""
    try:
        return json.loads(value, object_pairs_hook=_json_object, parse_constant=_invalid_constant)
    except json.JSONDecodeError as error:
        raise ValueError("Malformed JSON payload") from error


def parse_record_reference(value, targets: list[dict], pattern: str | None):
    """Return an unchanged target ID and normalized optional decimal version."""
    if value is None:
        return None, None, "absent"
    if not isinstance(value, str):
        return None, None, "invalid_reference"
    if pattern and not re.fullmatch(pattern, value, re.ASCII):
        return None, None, "invalid_reference"
    record_id, separator, terminal = value.rpartition(":")
    parts = record_id.split(":", 2)
    if not separator or len(parts) != 3 or not all(parts) or "--" not in parts[1]:
        return None, None, "invalid_reference"
    if terminal and not re.fullmatch(r"[0-9]+", terminal, re.ASCII):
        return None, None, "invalid_reference"
    group, entity = parts[1].split("--", 1)
    if targets and not any(
        (not target.get("GroupType") or target["GroupType"] == group)
        and (not target.get("EntityType") or target["EntityType"] == entity)
        for target in targets
    ):
        return None, None, "invalid_reference"
    version = (terminal.lstrip("0") or "0") if terminal else None
    return record_id, version, "pending"


def _record_pointer(path: tuple) -> str:
    return "/" + "/".join(segment.replace("~", "~0").replace("/", "~1") for segment in path)


def _collect_variant_references(value, plan: dict, path: tuple, ordinals: tuple, relationships: list):
    """Collect potential links by declared structure, without coercing JSON unions."""
    if value is not None and (
        plan["type"] == "object" and not isinstance(value, dict)
        or plan["type"] == "array" and not isinstance(value, list)
        or plan["type"] == "string" and not isinstance(value, str)
    ):
        return
    if plan.get("targets") is not None and (value is None or isinstance(value, str)):
        wanted_id, wanted_version, status = parse_record_reference(value, plan["targets"], plan.get("pattern"))
        relationships.append((_record_pointer(path), list(ordinals), value, wanted_id, wanted_version, status))
    if value is None:
        return
    if plan["type"] == "object":
        for name, child in plan["properties"].items():
            if name in value:
                _collect_variant_references(value[name], child, path + (name,), ordinals, relationships)
    elif plan["type"] == "array":
        for index, item in enumerate(value):
            _collect_variant_references(item, plan["items"], path + (str(index),), ordinals + (index,), relationships)
    elif plan["type"] == "json":
        for variant in plan.get("variants", []):
            _collect_variant_references(value, variant, path, ordinals, relationships)


def _project_value(value, plan: dict, path: tuple, ordinals: tuple, relationships: list):
    if plan.get("targets") is not None and (
        plan["type"] != "json" or value is None or isinstance(value, str)
    ):
        wanted_id, wanted_version, status = parse_record_reference(value, plan["targets"], plan.get("pattern"))
        raw = value if value is None or isinstance(value, str) else json.dumps(value, sort_keys=True)
        relationships.append((_record_pointer(path), list(ordinals), raw, wanted_id, wanted_version, status))
    if value is None:
        return None
    value_type = plan["type"]
    if value_type == "json":
        for variant in plan.get("variants", []):
            if _has_relationships(variant):
                _collect_variant_references(value, variant, path, ordinals, relationships)
        return json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":"))
    if value_type == "object":
        if not isinstance(value, dict):
            raise ValueError(f"Expected object at {'.'.join(path)}")
        return {
            name: _project_value(value.get(name), child, path + (name,), ordinals, relationships)
            if name in value else None
            for name, child in plan["properties"].items()
        }
    if value_type == "array":
        if not isinstance(value, list):
            raise ValueError(f"Expected array at {'.'.join(path)}")
        return [
            _project_value(item, plan["items"], path + (str(index),), ordinals + (index,), relationships)
            for index, item in enumerate(value)
        ]
    valid = (
        value_type == "string" and isinstance(value, str)
        or value_type == "boolean" and isinstance(value, bool)
        or value_type == "integer" and isinstance(value, int) and not isinstance(value, bool)
        and -(2**63) <= value < 2**63
        or value_type == "number" and isinstance(value, (int, float)) and not isinstance(value, bool)
        and math.isfinite(value)
    )
    if not valid:
        raise ValueError(f"Value does not match declared type at {'.'.join(path)}")
    return value


def project_record(fields: dict, plan: dict):
    """Project typed envelope fields and collect declared reference occurrences.

    Original ACZ columns remain untouched in the Spark output. Undeclared fields
    remain in those raw columns, not in inferred typed projections.
    """
    envelope = dict(fields)
    data = envelope.get("data")
    if isinstance(data, str):
        data = parse_json(data)
    if (
        isinstance(data, dict) and isinstance(data.get("data"), dict)
        and any(name in data for name in (
            "id", "kind", "meta", "createUser", "createTime", "modifyUser", "modifyTime",
        ))
    ):
        wrapper = data
        body_plan = plan["properties"]["data"]
        if "data" in body_plan.get("properties", {}):
            raise ValueError("Ambiguous payload: entity data also declares a data property")
        for name in ("id", "kind"):
            if name not in wrapper:
                continue
            if not isinstance(wrapper[name], str) or not wrapper[name]:
                raise ValueError(f"Storage wrapper {name} must be a non-empty string")
            if envelope.get(name) is not None and envelope[name] != wrapper[name]:
                raise ValueError(f"Storage wrapper {name} differs from the ACZ row")
        if wrapper.get("version") is not None and envelope.get("version") is not None:
            wrapper_version, row_version = str(wrapper["version"]), str(envelope["version"])
            if not re.fullmatch(r"[0-9]+", wrapper_version) or (
                wrapper_version.lstrip("0") or "0"
            ) != (row_version.lstrip("0") or "0"):
                raise ValueError("Storage wrapper version differs from the ACZ row")
        data = wrapper["data"]
        for name in plan["properties"]:
            if envelope.get(name) is None and name in wrapper:
                envelope[name] = wrapper[name]
    envelope["data"] = data
    projected = {}
    relationships = []
    for name, child in plan["properties"].items():
        if name in {"id", "version", "kind"}:
            continue
        if name not in envelope:
            projected[name] = None
            continue
        value = envelope.get(name)
        if isinstance(value, str) and child["type"] in {"object", "array", "json"}:
            value = parse_json(value)
        projected[name] = _project_value(value, child, (name,), (), relationships)
    occurrences = {}
    for occurrence in relationships:
        key = (occurrence[0], tuple(occurrence[1]), occurrence[2])
        if key not in occurrences or occurrence[-1] == "pending":
            occurrences[key] = occurrence
    return json.dumps(projected, allow_nan=False, separators=(",", ":")), list(occurrences.values())
