"""Notebook synchronization and validation helpers for local development."""

from __future__ import annotations

import argparse
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

    return issues


def validate_notebook(notebook: dict[str, Any]) -> None:
    issues = validation_issues(notebook)
    if issues:
        raise ValueError("\n".join(issues))


def sync_notebook(path: str | Path, check: bool = False) -> bool:
    notebook_path = Path(path)
    original = load_notebook(notebook_path)
    cleaned = clean_notebook(original)
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
    parser = argparse.ArgumentParser(description="Validate and normalize the ADME ACZ Silver Layer notebook.")
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
