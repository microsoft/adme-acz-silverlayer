"""Naming and kind-selection helpers for Silver Layer outputs."""

from __future__ import annotations

import re

DATA_PREFIX = "data__"


def kind_to_table_name(kind: str) -> str:
    try:
        authority, source, entity_ver = kind.split(":", 2)
        entity, _ = entity_ver.rsplit(":", 1)
    except ValueError:
        return sanitize_table_name_part(kind)
    entity_base = entity.split("--")[-1]
    return "_".join(
        sanitize_table_name_part(part)
        for part in (authority, source, entity_base)
    )


def sanitize_table_name_part(value: str) -> str:
    clean = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value).strip())
    clean = "_".join(part for part in clean.split("_") if part)
    return clean or "field"


def child_table_suffix(field_path: str) -> str:
    normalized = str(field_path).replace(".", "__")
    if normalized.startswith(DATA_PREFIX):
        normalized = normalized[len(DATA_PREFIX) :]
    parts = [sanitize_table_name_part(part) for part in normalized.split("__") if part]
    return "__".join(parts) if parts else "items"


def child_table_name(parent: str, array_field: str) -> str:
    return f"{parent}___{child_table_suffix(array_field)}"


def kind_parts(kind: str) -> dict[str, str]:
    authority, source, entity_ver = kind.split(":", 2)
    entity, version = entity_ver.rsplit(":", 1)
    return {
        "authority": authority,
        "source": source,
        "entity": entity,
        "entity_base": entity.split("--")[-1],
        "version": version,
    }


def kind_family_key(kind: str) -> str:
    p = kind_parts(kind)
    return f"{p['authority']}:{p['source']}:{p['entity']}"


def kind_version(kind: str) -> str:
    return kind_parts(kind)["version"]


def kind_to_versioned_table_name(kind: str) -> str:
    base = kind_to_table_name(kind)
    version = kind_version(kind).replace(".", "_").replace("-", "_")
    return f"{base}__v{version}"


def group_kinds_by_version_strategy(kinds: list[str], version_strategy: str) -> list[dict[str, object]]:
    if version_strategy == "versioned_tables":
        return [
            {
                "group_key": f"{kind_family_key(kind)}:{kind_version(kind)}",
                "kinds": [kind],
                "parent_table_base": kind_to_versioned_table_name(kind),
                "versions": [kind_version(kind)],
            }
            for kind in kinds
        ]

    groups: dict[str, dict[str, object]] = {}
    for kind in kinds:
        key = kind_family_key(kind)
        group = groups.setdefault(
            key,
            {
                "group_key": key,
                "kinds": [],
                "parent_table_base": kind_to_table_name(kind),
                "versions": [],
            },
        )
        group["kinds"].append(kind)
        group["versions"].append(kind_version(kind))

    for group in groups.values():
        group["kinds"] = sorted(group["kinds"], key=kind_version)
        group["versions"] = sorted(set(group["versions"]))
    return list(groups.values())


def table_name_for_kind_group(group: dict[str, object], table_prefix: str) -> str:
    return f"{table_prefix}{group['parent_table_base']}"


def detect_table_collisions(kinds: list[str], table_prefix: str, version_strategy: str) -> list[dict[str, object]]:
    rows: dict[str, list[str]] = {}
    for group in group_kinds_by_version_strategy(kinds, version_strategy):
        table_name = table_name_for_kind_group(group, table_prefix)
        rows.setdefault(table_name, []).extend(group["kinds"])
    return [
        {
            "table_name": table_name,
            "kinds": table_kinds,
            "safe": version_strategy == "merge" and len({kind_family_key(k) for k in table_kinds}) == 1,
        }
        for table_name, table_kinds in rows.items()
        if len(table_kinds) > 1
    ]


def is_all_kinds_selector(value: str | None) -> bool:
    selector = (value or "").strip()
    return selector == "*" or selector == "*:*:*:*" or selector.lower() == "all"


def is_kind_pattern(value: str | None) -> bool:
    selector = (value or "").strip()
    return is_all_kinds_selector(selector) or "*" in selector


def kind_pattern_to_regex(pattern: str):
    escaped = re.escape(pattern.strip())
    return re.compile("^" + escaped.replace(r"\*", ".*") + "$")


def matches_kind_selector(kind: str, selector: str) -> bool:
    selector = selector.strip()
    if is_all_kinds_selector(selector):
        return True
    if "*" in selector:
        return bool(kind_pattern_to_regex(selector).match(kind))
    return kind == selector


def clean_kind_selectors(selectors: list[str]) -> list[str]:
    cleaned = [str(selector).strip() for selector in selectors if str(selector).strip()]
    if not cleaned:
        raise ValueError("At least one OSDU kind or wildcard selector is required.")
    return list(dict.fromkeys(cleaned))


def clean_optional_kind_selectors(selectors: list[str] | str | None) -> list[str]:
    if selectors in (None, ""):
        return []
    raw_selectors = [part.strip() for part in selectors.split(",")] if isinstance(selectors, str) else selectors
    cleaned = [str(selector).strip() for selector in raw_selectors if str(selector).strip()]
    return list(dict.fromkeys(cleaned))


def filter_excluded_kinds(kinds: list[str], excluded_selectors: list[str] | str | None = None) -> list[str]:
    resolved = list(dict.fromkeys(kinds))
    cleaned_excluded = clean_optional_kind_selectors(excluded_selectors)
    if not cleaned_excluded:
        return resolved
    return [
        kind
        for kind in resolved
        if not any(matches_kind_selector(kind, selector) for selector in cleaned_excluded)
    ]


def kind_selectors_require_discovery(selectors: list[str]) -> bool:
    return any(is_kind_pattern(selector) for selector in selectors)
