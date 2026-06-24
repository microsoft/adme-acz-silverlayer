import ast
import json
import os
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "ADME ACZ Silver Layer.ipynb"


def load_notebook() -> dict:
    return json.loads(NOTEBOOK.read_text(encoding="utf-8"))


def notebook_source(nb: dict, cell_type: str | None = None) -> str:
    parts: list[str] = []
    for cell in nb["cells"]:
        if cell_type is None or cell["cell_type"] == cell_type:
            parts.append("".join(cell.get("source", [])))
    return "\n".join(parts)


def markdown_headings(nb: dict) -> list[tuple[int, str]]:
    headings: list[tuple[int, str]] = []
    for index, cell in enumerate(nb["cells"]):
        if cell["cell_type"] != "markdown":
            continue
        for line in "".join(cell.get("source", [])).splitlines():
            if line.startswith("#"):
                headings.append((index, line.strip()))
    return headings


def extract_function(nb: dict, function_name: str):
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        tree = ast.parse("".join(cell.get("source", [])))
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == function_name:
                module = ast.Module(body=[node], type_ignores=[])
                ast.fix_missing_locations(module)
                namespace: dict[str, object] = {"json": json, "os": os}
                exec(compile(module, filename=f"<{function_name}>", mode="exec"), namespace)
                return namespace[function_name]
    raise AssertionError(f"Function {function_name!r} was not found")


def extract_functions(nb: dict, function_names: list[str]) -> dict[str, object]:
    wanted = set(function_names)
    nodes: list[ast.FunctionDef] = []
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
    namespace: dict[str, object] = {"json": json, "os": os, "re": re}
    exec(compile(module, filename="<notebook-functions>", mode="exec"), namespace)
    return {name: namespace[name] for name in function_names}


class NotebookSimplificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.nb = load_notebook()

    def test_notebook_structure_is_valid(self) -> None:
        self.assertEqual(self.nb["nbformat"], 4)
        self.assertGreaterEqual(len(self.nb["cells"]), 20)
        self.assertTrue(all(cell["cell_type"] in {"markdown", "code"} for cell in self.nb["cells"]))
        self.assertTrue(all(not cell.get("outputs") for cell in self.nb["cells"] if cell["cell_type"] == "code"))

    def test_setup_and_smoke_tests_precede_pipeline_execution(self) -> None:
        heading_positions = dict(markdown_headings(self.nb))
        setup = next(index for index, heading in heading_positions.items() if heading == "## Setup checklist")
        smoke = next(index for index, heading in heading_positions.items() if heading == "## Smoke test bronze access")
        run = next(index for index, heading in heading_positions.items() if heading == "## Run pipeline")
        results = next(index for index, heading in heading_positions.items() if heading == "## Results summary")
        self.assertLess(setup, smoke)
        self.assertLess(smoke, run)
        self.assertLess(run, results)

    def test_output_mode_controls_are_present(self) -> None:
        source = notebook_source(self.nb, "code")
        self.assertIn('OUTPUT_MODE = "normalized"', source)
        self.assertIn("ADME_OUTPUT_MODE", source)
        self.assertIn("ADME_REASSEMBLE", source)
        self.assertIn("Output mode", source)
        self.assertIn('RUN_PROFILE = "interactive"', source)
        self.assertIn('"dry_run"', source)

    def test_explicit_tenant_config_and_repeatability_controls_are_present(self) -> None:
        source = notebook_source(self.nb, "code")
        for expected in [
            'WORKSPACE_ID = ""',
            'LAKEHOUSE_ID = ""',
            'BRONZE_TABLE = "osducatalog"',
            'NOTEBOOK_VERSION = "0.2.0"',
            "ALLOW_OVERWRITE = False",
            "ADME_ALLOW_OVERWRITE",
            "config_hash = hashlib.sha256",
            "PERSIST_SCHEMA_CACHE = True",
            'SCHEMA_CACHE_TABLE = "silver_schema_cache"',
            'RUN_MANIFEST_TABLE = "silver_run_manifest"',
            'schema_cache_writes_enabled = persist_schema_cache and run_profile == "full"',
            "ADME_PERSIST_SCHEMA_CACHE",
            "ADME_SCHEMA_CACHE_TABLE",
            "ADME_RUN_MANIFEST_TABLE",
            "KIND_LIMITS = {}",
            "ADME_KIND_LIMITS",
            "kind_selectors",
            "WRITE_OUTPUT_DOCS = True",
            'OUTPUT_DOCS_TABLE = "silver_output_documentation"',
            "ADME_WRITE_OUTPUT_DOCS",
            "ADME_OUTPUT_DOCS_TABLE",
        ]:
            self.assertIn(expected, source)

    def test_setup_checklist_dry_run_and_manifest_are_present(self) -> None:
        source = notebook_source(self.nb, "code")
        for expected in [
            "def run_setup_checklist(",
            "def run_silver_dry_run(",
            "def preview_output_tables(",
            "def limit_for_kind(",
            "def is_all_kinds_selector(",
            "def kind_pattern_to_regex(",
            "def matches_kind_selector(",
            "def discover_bronze_kinds(",
            "def resolve_kind_selectors(",
            "def ensure_resolved_kinds(",
            "def read_bronze_table_spark(",
            "def write_run_manifest(",
            "def write_output_documentation(",
            "def validate_table_names(",
            "def _assert_overwrite_allowed(",
            "RUN_MANIFEST_SCHEMA",
            "SCHEMA_CACHE_SCHEMA",
            "OUTPUT_DOCS_SCHEMA",
            'T.StructField("notebook_version", T.StringType(), True)',
            'T.StructField("config_hash", T.StringType(), True)',
            'T.StructField("allow_overwrite", T.BooleanType(), True)',
            'T.StructField("table_role", T.StringType(), False)',
            'T.StructField("column_name", T.StringType(), False)',
            "_load_schema_doc_from_persistent_cache",
            "_write_schema_doc_to_persistent_cache",
            "silver_output_documentation",
        ]:
            self.assertIn(expected, source)

    def test_full_run_receives_overwrite_and_metadata_arguments(self) -> None:
        source = notebook_source(self.nb, "code")
        self.assertIn("allow_overwrite=allow_overwrite", source)
        self.assertIn("notebook_version=NOTEBOOK_VERSION", source)
        self.assertIn("config_hash=config_hash", source)
        self.assertIn("Full refresh would overwrite existing table(s)", source)
        self.assertIn("Set ALLOW_OVERWRITE = True", source)

    def test_no_hardcoded_environment_ids_or_driver_collected_changed_ids(self) -> None:
        raw = NOTEBOOK.read_text(encoding="utf-8")
        self.assertIsNone(
            re.search(
                r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
                raw,
            )
        )
        source = notebook_source(self.nb, "code")
        self.assertNotIn("changed_ids = [r[0]", source)
        self.assertNotIn(".isin(changed_ids)", source)

    def test_output_mode_normalization(self) -> None:
        normalize_output_mode = extract_function(self.nb, "_normalize_output_mode")
        self.assertEqual(normalize_output_mode(None), "normalized")
        self.assertEqual(normalize_output_mode("normalized"), "normalized")
        self.assertEqual(normalize_output_mode("parent-children"), "normalized")
        self.assertEqual(normalize_output_mode("wide"), "wide")
        self.assertEqual(normalize_output_mode("reassembled"), "wide")
        with self.assertRaises(ValueError):
            normalize_output_mode("unsupported")

    def test_env_bool(self) -> None:
        env_bool = extract_function(self.nb, "_env_bool")
        self.assertTrue(env_bool("MISSING_ENV_VALUE", True))
        self.assertFalse(env_bool("MISSING_ENV_VALUE", False))

    def test_kind_limit_parser(self) -> None:
        parse_kind_limits = extract_function(self.nb, "_parse_kind_limits")
        self.assertEqual(parse_kind_limits(None), {})
        self.assertEqual(parse_kind_limits({"WellLog": 10}), {"WellLog": 10})
        self.assertEqual(parse_kind_limits('{"WellLog": 10}'), {"WellLog": 10})
        self.assertEqual(parse_kind_limits("WellLog=10;wellboretrajectory=5"), {"WellLog": 10, "wellboretrajectory": 5})
        with self.assertRaises(ValueError):
            parse_kind_limits("WellLog")
        with self.assertRaises(ValueError):
            parse_kind_limits({"WellLog": -1})

    def test_wildcard_kind_selectors(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "is_all_kinds_selector",
                "is_kind_pattern",
                "kind_pattern_to_regex",
                "matches_kind_selector",
                "_clean_kind_selectors",
                "kind_selectors_require_discovery",
            ],
        )
        is_all_kinds_selector = funcs["is_all_kinds_selector"]
        is_kind_pattern = funcs["is_kind_pattern"]
        kind_pattern_to_regex = funcs["kind_pattern_to_regex"]
        matches_kind_selector = funcs["matches_kind_selector"]
        clean_kind_selectors = funcs["_clean_kind_selectors"]
        kind_selectors_require_discovery = funcs["kind_selectors_require_discovery"]

        welllog = "osdu:wks:work-product-component--WellLog:1.4.0"
        wellbore = "osdu:wks:master-data--Wellbore:1.2.0"

        self.assertTrue(is_all_kinds_selector("*:*:*:*"))
        self.assertTrue(is_all_kinds_selector("*"))
        self.assertTrue(is_all_kinds_selector("ALL"))
        self.assertTrue(is_kind_pattern("osdu:wks:*:*"))
        self.assertTrue(kind_pattern_to_regex("*:wks:work-product-component--Well*:1.*").match(welllog))
        self.assertTrue(matches_kind_selector(welllog, "*:wks:work-product-component--Well*:1.*"))
        self.assertFalse(matches_kind_selector(wellbore, "*:wks:work-product-component--Well*:1.*"))
        self.assertEqual(clean_kind_selectors([" ", welllog, welllog, "all"]), [welllog, "all"])
        self.assertTrue(kind_selectors_require_discovery([welllog, "osdu:wks:*:*"]))
        self.assertFalse(kind_selectors_require_discovery([welllog]))

    def test_kind_to_table_name(self) -> None:
        kind_to_table_name = extract_function(self.nb, "kind_to_table_name")
        self.assertEqual(
            kind_to_table_name("osdu:wks:work-product-component--WellLog:1.4.0"),
            "welllog",
        )
        self.assertEqual(
            kind_to_table_name("osdu:wks:master-data--Well:1.2.0"),
            "well",
        )
        self.assertEqual(kind_to_table_name("Custom-Entity.Name"), "custom_entity_name")


if __name__ == "__main__":
    unittest.main()
