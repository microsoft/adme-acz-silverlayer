import ast
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
NOTEBOOK = ROOT / "ADME ACZ Silver Layer.ipynb"

sys.path.insert(0, str(SRC))

from adme_acz_silverlayer import metadata, runtime  # noqa: E402


def load_notebook() -> dict:
    return json.loads(NOTEBOOK.read_text(encoding="utf-8"))


def extract_functions(function_names: list[str]) -> dict[str, object]:
    wanted = set(function_names)
    nodes: list[ast.FunctionDef] = []
    nb = load_notebook()
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        tree = ast.parse("".join(cell.get("source", [])))
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in wanted:
                nodes.append(node)

    found = {node.name for node in nodes}
    missing = wanted - found
    if missing:
        raise AssertionError(f"Function(s) not found: {sorted(missing)}")

    module = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    namespace: dict[str, object] = {
        "json": json,
        "KindResult": object,
        "MERGE_KEY_COLUMNS": ["id", "version"],
    }
    exec(compile(module, filename="<runtime-metadata-functions>", mode="exec"), namespace)
    return {name: namespace[name] for name in function_names}


class RuntimeMetadataModuleTests(unittest.TestCase):
    def test_runtime_helpers_match_notebook(self) -> None:
        funcs = extract_functions(
            [
                "_effective_merge_key_columns",
                "_merge_condition",
                "_schema_fetch_parallelism",
                "_wide_max_cardinality_cap",
                "_watermark_active",
                "validate_incremental_limit_safety",
                "_retry_skipped_schema_records",
            ]
        )

        self.assertEqual(runtime.effective_merge_key_columns(["id", "version"]), funcs["_effective_merge_key_columns"](["id", "version"]))
        self.assertEqual(runtime.effective_merge_key_columns([]), funcs["_effective_merge_key_columns"]([]))
        with self.assertRaises(ValueError):
            runtime.effective_merge_key_columns([" "])
        with self.assertRaises(ValueError):
            funcs["_effective_merge_key_columns"]([" "])

        self.assertEqual(runtime.merge_condition("target", "source", ["id", "version"]), funcs["_merge_condition"]("target", "source", ["id", "version"]))
        self.assertEqual(runtime.schema_fetch_parallelism(), funcs["_schema_fetch_parallelism"]())
        self.assertEqual(runtime.wide_max_cardinality_cap(), funcs["_wide_max_cardinality_cap"]())
        self.assertEqual(runtime.watermark_active(True, "ingestTime", "auto"), funcs["_watermark_active"](True, "ingestTime", "auto"))
        self.assertEqual(runtime.watermark_active(True, "ingestTime", "off"), funcs["_watermark_active"](True, "ingestTime", "off"))
        self.assertEqual(runtime.retry_skipped_schema_records(), funcs["_retry_skipped_schema_records"]())

        runtime.validate_incremental_limit_safety(True, "ingestTime", "auto", None, {})
        funcs["validate_incremental_limit_safety"](True, "ingestTime", "auto", None, {})
        with self.assertRaisesRegex(ValueError, "Watermark-based upsert cannot run with LIMIT"):
            runtime.validate_incremental_limit_safety(True, "ingestTime", "auto", 10, {})
        with self.assertRaisesRegex(ValueError, "Watermark-based upsert cannot run with LIMIT"):
            funcs["validate_incremental_limit_safety"](True, "ingestTime", "auto", 10, {})

    def test_metadata_helpers_match_notebook(self) -> None:
        funcs = extract_functions(
            [
                "_timings_json",
                "_output_tables_from_results",
                "_data_quality_enabled",
                "_data_quality_max_examples",
                "_data_quality_issues_table_name",
            ]
        )

        timings = {"write": 1.23456, "read": 0.1004}
        self.assertEqual(metadata.timings_json(timings), funcs["_timings_json"](timings))
        self.assertEqual(metadata.data_quality_enabled(), funcs["_data_quality_enabled"]())
        self.assertEqual(metadata.data_quality_max_examples(), funcs["_data_quality_max_examples"]())
        self.assertEqual(metadata.data_quality_issues_table_name(), funcs["_data_quality_issues_table_name"]())

        class Result:
            def __init__(self, parent_table: str, child_tables: list[str] | None = None) -> None:
                self.parent_table = parent_table
                self.child_tables = child_tables

        results = [
            Result("welllog", ["welllog___curves", "welllog___curves"]),
            Result("welllog", ["welllog___parameters"]),
        ]
        self.assertEqual(metadata.output_tables_from_results(results), funcs["_output_tables_from_results"](results))
        self.assertEqual(metadata.output_tables_from_results([], ["planned_parent"]), funcs["_output_tables_from_results"]([], ["planned_parent"]))


if __name__ == "__main__":
    unittest.main()
