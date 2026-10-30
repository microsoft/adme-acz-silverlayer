"""Execute the committed Fabric notebook end to end against a local Spark session."""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "ADME ACZ Silver Layer.ipynb"
SETUP_CELL_MARKER = "# Setup checklist: validate configuration"
SETTINGS_CELL_MARKER = "CUSTOMER SETTINGS"


def _override_setting(source: str, name: str, value) -> str:
    match = re.search(rf"^{name} = [^\n]*", source, flags=re.M)
    if not match:
        raise KeyError(f"Notebook setting {name} not found")
    end = match.end()
    if match.group(0).rstrip().endswith("["):
        end = source.index("\n]", match.start()) + 2
    return source[:match.start()] + f"{name} = {value!r}" + source[end:]


def run_notebook(spark, settings: dict, before_pipeline=None, displayed=None) -> dict:
    """Run every code cell and return the notebook namespace.

    Output paths use the local Spark warehouse before the caller hook runs.
    `before_pipeline(namespace)` runs after all helpers are defined and before the first
    cell that touches Spark data or ADME, so callers can replace the schema service.
    """
    namespace = {"spark": spark, "__name__": "__main__"}
    namespace["display"] = (lambda frame: displayed.append(frame)) if displayed is not None else (lambda frame: None)
    hooked = False
    for cell in json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"]:
        if cell["cell_type"] != "code":
            continue
        source = "".join(cell["source"])
        if SETTINGS_CELL_MARKER in source:
            for name, value in settings.items():
                source = _override_setting(source, name, value)
        if not hooked and SETUP_CELL_MARKER in source:
            hooked = True
            warehouse = spark.conf.get("spark.sql.warehouse.dir").rstrip("/")
            namespace["_table_path_uri"] = lambda table: f"{warehouse}/{table}"
            if before_pipeline:
                before_pipeline(namespace)
        exec(compile(source, "<notebook-cell>", "exec"), namespace)
    if not hooked:
        raise RuntimeError("Setup checklist cell not found; notebook structure changed.")
    return namespace
