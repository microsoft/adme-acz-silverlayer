"""Notebook synchronization and validation helpers for local development."""

from __future__ import annotations

import argparse
import ast
import copy
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

NOTEBOOK_NAME = "ADME ACZ Silver Layer.ipynb"

EXPECTED_HEADINGS = (
    "# ADME ACZ Silver Layer",
    "## Architecture",
    "## Spark runtime configuration",
    "## Configuration",
    "## Pipeline constants",
    "## Helper functions",
    "## Core decomposition and reassembly logic",
    "## Pipeline functions",
    "## Setup checklist",
    "## Smoke test bronze access",
    "## Run pipeline",
    "## Results summary",
)

LOCAL_PACKAGE_IMPORT_RE = re.compile(
    r"^\s*(?:from\s+adme_acz_silverlayer\b|import\s+adme_acz_silverlayer\b)",
    re.MULTILINE,
)

SHARED_SPARK_HELPERS = {
    "fabric_io": (
        "_lakehouse_default_schema",
        "_lakehouse_table_path",
    ),
    "config": (
        "NOTEBOOK_PARAMETER_TYPES",
        "_parameter_bool",
        "_parameter_int",
        "_parameter_value",
        "resolve_notebook_parameters",
    ),
    "spark_schema": (
        "_JSON_TYPE_MAP",
        "_resolve_node",
        "_json_schema_to_spark",
        "_classify_spark_type",
        "_parse_osdu_schema",
        "_with_struct_field_type",
        "_merge_struct_fields",
        "_dedupe_case_insensitive_struct_type",
        "_merge_struct_types",
        "_merge_schemas",
    ),
    "normalization": (
        "record_key",
        "_DELTA_COLUMN_PART_RE",
        "_sanitize_column_name_part",
        "sanitize_delta_column_name",
        "make_delta_column_alias",
        "_quoted_top_level_col",
        "_nested_field_col",
        "_flatten_typed_structs",
        "_flatten_all_struct_columns",
        "explode_array",
    ),
}


def synchronize_shared_helpers(notebook: dict[str, Any]) -> dict[str, Any]:
    """Embed canonical helper definitions without importing the package in Fabric."""
    definitions: dict[str, str] = {}
    for module, names in SHARED_SPARK_HELPERS.items():
        source = Path(__file__).with_name(f"{module}.py").read_text(encoding="utf-8")
        nodes = {}
        for node in ast.parse(source).body:
            if isinstance(node, ast.FunctionDef):
                nodes[node.name] = node
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                nodes[node.target.id] = node
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        nodes[target.id] = node
        for name in names:
            if name not in nodes:
                raise ValueError(f"Shared helper {name!r} is missing from {module}.py.")
            definitions[name] = ast.get_source_segment(source, nodes[name]) + "\n"

    synchronized = copy.deepcopy(notebook)
    locations: dict[str, tuple[int, ast.AST]] = {}
    for index, cell in enumerate(synchronized.get("cells", [])):
        if cell.get("cell_type") != "code":
            continue
        for node in ast.parse("".join(cell.get("source", []))).body:
            if isinstance(node, ast.FunctionDef):
                names = [node.name]
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                names = [node.target.id]
            elif isinstance(node, ast.Assign):
                names = [target.id for target in node.targets if isinstance(target, ast.Name)]
            else:
                continue
            for name in names:
                if name in locations and name in definitions:
                    raise ValueError(f"Shared helper {name!r} occurs more than once in the notebook.")
                locations[name] = (index, node)

    missing = set(definitions) - set(locations)
    if missing - {"explode_array"}:
        raise ValueError(f"Notebook is missing shared helper(s): {', '.join(sorted(missing))}.")
    edits: dict[int, list[tuple[int, int, str]]] = {}
    for name, definition in definitions.items():
        if name in locations:
            index, node = locations[name]
            edits.setdefault(index, []).append((node.lineno - 1, node.end_lineno, definition))
        else:
            if "_build_child_primitive" not in locations:
                raise ValueError("Notebook is missing the child-array helper insertion point.")
            index, anchor = locations["_build_child_primitive"]
            edits.setdefault(index, []).append((anchor.lineno - 1, anchor.lineno - 1, definition + "\n\n"))

    for index, replacements in edits.items():
        lines = "".join(synchronized["cells"][index]["source"]).splitlines(keepends=True)
        for start, end, definition in sorted(replacements, reverse=True):
            lines[start:end] = definition.splitlines(keepends=True)
        synchronized["cells"][index]["source"] = lines
    return synchronized


@dataclass(frozen=True)
class NotebookSummary:
    path: Path
    cells: int
    code_cells: int
    markdown_cells: int
    code_lines: int
    headings: tuple[str, ...]


def default_notebook_path(root: Path | None = None) -> Path:
    base = Path.cwd() if root is None else root
    return base / NOTEBOOK_NAME


def load_notebook(path: str | Path) -> dict[str, Any]:
    notebook_path = Path(path)
    return json.loads(notebook_path.read_text(encoding="utf-8"))


def dump_notebook(notebook: dict[str, Any]) -> str:
    return json.dumps(notebook, ensure_ascii=False, indent=1) + "\n"


def write_notebook(path: str | Path, notebook: dict[str, Any]) -> None:
    Path(path).write_text(dump_notebook(notebook), encoding="utf-8")


def clean_notebook(notebook: dict[str, Any]) -> dict[str, Any]:
    cleaned = copy.deepcopy(notebook)
    for cell in cleaned.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        cell["execution_count"] = None
        cell["outputs"] = []
    return cleaned


def notebook_is_clean(notebook: dict[str, Any]) -> bool:
    return clean_notebook(notebook) == notebook


def markdown_headings(notebook: dict[str, Any]) -> tuple[str, ...]:
    headings: list[str] = []
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "markdown":
            continue
        source = "".join(cell.get("source", []))
        headings.extend(line.strip() for line in source.splitlines() if line.startswith("#"))
    return tuple(headings)


def notebook_source(notebook: dict[str, Any], cell_type: str | None = None) -> str:
    parts: list[str] = []
    for cell in notebook.get("cells", []):
        if cell_type is None or cell.get("cell_type") == cell_type:
            parts.append("".join(cell.get("source", [])))
    return "\n".join(parts)


def summarize_notebook(path: str | Path, notebook: dict[str, Any] | None = None) -> NotebookSummary:
    notebook_path = Path(path)
    nb = load_notebook(notebook_path) if notebook is None else notebook
    code_cells = [cell for cell in nb.get("cells", []) if cell.get("cell_type") == "code"]
    markdown_cells = [cell for cell in nb.get("cells", []) if cell.get("cell_type") == "markdown"]
    return NotebookSummary(
        path=notebook_path,
        cells=len(nb.get("cells", [])),
        code_cells=len(code_cells),
        markdown_cells=len(markdown_cells),
        code_lines=sum(len("".join(cell.get("source", [])).splitlines()) for cell in code_cells),
        headings=markdown_headings(nb),
    )


def validation_issues(notebook: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if notebook.get("nbformat") != 4:
        issues.append("Notebook nbformat must be 4.")

    for index, cell in enumerate(notebook.get("cells", []), start=1):
        cell_type = cell.get("cell_type")
        if cell_type not in {"code", "markdown"}:
            issues.append(f"Cell {index} has unsupported cell_type {cell_type!r}.")
        if cell_type == "code":
            if cell.get("outputs"):
                issues.append(f"Code cell {index} contains outputs.")
            if cell.get("execution_count") is not None:
                issues.append(f"Code cell {index} contains an execution_count.")

    headings = markdown_headings(notebook)
    heading_positions = {heading: i for i, heading in enumerate(headings)}
    missing_headings = [heading for heading in EXPECTED_HEADINGS if heading not in heading_positions]
    if missing_headings:
        issues.append(f"Notebook is missing expected heading(s): {', '.join(missing_headings)}.")
    else:
        positions = [heading_positions[heading] for heading in EXPECTED_HEADINGS]
        if positions != sorted(positions):
            issues.append("Notebook headings are not in the expected execution order.")

    code_source = notebook_source(notebook, "code")
    if LOCAL_PACKAGE_IMPORT_RE.search(code_source):
        issues.append("Customer notebook must not import adme_acz_silverlayer at runtime.")

    if synchronize_shared_helpers(notebook) != notebook:
        issues.append("Shared Spark helpers differ from the package source; run scripts/sync_notebook.py.")

    return issues


def validate_notebook(notebook: dict[str, Any]) -> None:
    issues = validation_issues(notebook)
    if issues:
        raise ValueError("\n".join(issues))


def sync_notebook(path: str | Path, check: bool = False) -> bool:
    notebook_path = Path(path)
    original = load_notebook(notebook_path)
    cleaned = synchronize_shared_helpers(clean_notebook(original))
    validate_notebook(cleaned)

    changed = cleaned != original
    if changed and not check:
        write_notebook(notebook_path, cleaned)
    return changed


def _format_summary(summary: NotebookSummary) -> str:
    return (
        f"{summary.path}: {summary.cells} cells, {summary.code_cells} code cells, "
        f"{summary.markdown_cells} markdown cells, {summary.code_lines} code lines"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Embed shared helpers and normalize the ADME ACZ Silver Layer notebook.")
    parser.add_argument(
        "notebook",
        nargs="?",
        default=NOTEBOOK_NAME,
        help=f"Notebook path. Defaults to {NOTEBOOK_NAME!r}.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate only and fail if synchronization would modify the notebook.",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print notebook cell and heading summary.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    notebook_path = Path(args.notebook)

    try:
        changed = sync_notebook(notebook_path, check=args.check)
        if args.summary:
            print(_format_summary(summarize_notebook(notebook_path)))
        if args.check and changed:
            print(f"{notebook_path} is not synchronized.", file=sys.stderr)
            return 1
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
