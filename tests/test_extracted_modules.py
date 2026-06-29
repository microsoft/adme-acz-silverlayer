import ast
import json
import re
import sys
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
NOTEBOOK = ROOT / "ADME ACZ Silver Layer.ipynb"

sys.path.insert(0, str(SRC))

from adme_acz_silverlayer import config, naming, schema  # noqa: E402


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
        "os": __import__("os"),
        "re": re,
        "Any": Any,
        "MERGE_KEY_COLUMNS": ["id", "version"],
        "ADME_SCHEMA_RETRY_STATUS_CODES": [408, 429, 500, 502, 503, 504],
        "_DATA_PREFIX": "data__",
    }
    exec(compile(module, filename="<notebook-module-sync>", mode="exec"), namespace)
    return {name: namespace[name] for name in function_names}


class ExtractedModuleTests(unittest.TestCase):
    def test_config_helpers_match_notebook(self) -> None:
        funcs = extract_functions(
            [
                "_normalize_output_mode",
                "_normalize_version_strategy",
                "_normalize_missing_schema_mode",
                "_normalize_output_docs_mode",
                "_normalize_write_mode",
                "_normalize_watermark_mode",
                "_parse_retry_status_codes",
                "_parse_kind_limits",
                "_parse_merge_key_columns",
            ]
        )

        for value in [None, "normalized", "parent-children", "wide", "reassembled"]:
            self.assertEqual(config.normalize_output_mode(value), funcs["_normalize_output_mode"](value))
        for value in ["merge", "versioned_tables"]:
            self.assertEqual(config.normalize_version_strategy(value), funcs["_normalize_version_strategy"](value))
        for value in ["skip", "infer", "fail"]:
            self.assertEqual(config.normalize_missing_schema_mode(value), funcs["_normalize_missing_schema_mode"](value))
        for value in ["off", "summary", "full"]:
            self.assertEqual(config.normalize_output_docs_mode(value), funcs["_normalize_output_docs_mode"](value))
        for value, incremental in [("", False), ("", True), ("incremental", False), ("overwrite", True)]:
            self.assertEqual(config.normalize_write_mode(value, incremental), funcs["_normalize_write_mode"](value, incremental))
        for value in [None, "off", "auto", "required"]:
            self.assertEqual(config.normalize_watermark_mode(value), funcs["_normalize_watermark_mode"](value))

        self.assertEqual(config.parse_retry_status_codes("429,500,503"), funcs["_parse_retry_status_codes"]("429,500,503"))
        self.assertEqual(config.parse_kind_limits("WellLog=10;Trajectory=5"), funcs["_parse_kind_limits"]("WellLog=10;Trajectory=5"))
        self.assertEqual(config.parse_merge_key_columns("id,version"), funcs["_parse_merge_key_columns"]("id,version"))
        self.assertEqual(config.bounded_positive_int("4", 1, "TEST_VALUE"), 4)
        with self.assertRaises(ValueError):
            config.bounded_positive_int("0", 1, "TEST_VALUE")

    def test_naming_helpers_match_notebook(self) -> None:
        funcs = extract_functions(
            [
                "kind_to_table_name",
                "_sanitize_table_name_part",
                "_child_table_suffix",
                "child_table_name",
                "kind_parts",
                "kind_family_key",
                "kind_version",
                "kind_to_versioned_table_name",
                "group_kinds_by_version_strategy",
                "table_name_for_kind_group",
                "detect_table_collisions",
                "is_all_kinds_selector",
                "is_kind_pattern",
                "kind_pattern_to_regex",
                "matches_kind_selector",
                "_clean_kind_selectors",
                "kind_selectors_require_discovery",
            ]
        )
        welllog = "osdu:wks:work-product-component--WellLog:1.4.0"
        organisation_v1 = "osdu:wks:master-data--Organisation:1.0.0"
        organisation_v2 = "osdu:wks:master-data--Organisation:1.2.0"

        self.assertEqual(naming.kind_to_table_name(welllog), funcs["kind_to_table_name"](welllog))
        self.assertEqual(naming.sanitize_table_name_part("A-B.C"), funcs["_sanitize_table_name_part"]("A-B.C"))
        self.assertEqual(naming.child_table_suffix("data__LogData__Curves"), funcs["_child_table_suffix"]("data__LogData__Curves"))
        self.assertEqual(naming.child_table_name("welllog", "data__LogData__Curves"), funcs["child_table_name"]("welllog", "data__LogData__Curves"))
        self.assertEqual(naming.kind_parts(welllog), funcs["kind_parts"](welllog))
        self.assertEqual(naming.kind_family_key(welllog), funcs["kind_family_key"](welllog))
        self.assertEqual(naming.kind_version(welllog), funcs["kind_version"](welllog))
        self.assertEqual(naming.kind_to_versioned_table_name(organisation_v2), funcs["kind_to_versioned_table_name"](organisation_v2))

        kinds = [organisation_v1, organisation_v2]
        self.assertEqual(naming.group_kinds_by_version_strategy(kinds, "merge"), funcs["group_kinds_by_version_strategy"](kinds, "merge"))
        self.assertEqual(naming.detect_table_collisions(kinds, "", "merge"), funcs["detect_table_collisions"](kinds, "", "merge"))
        self.assertEqual(naming.clean_kind_selectors([" ", welllog, welllog, "all"]), funcs["_clean_kind_selectors"]([" ", welllog, welllog, "all"]))
        self.assertEqual(naming.kind_selectors_require_discovery([welllog, "osdu:wks:*:*"]), funcs["kind_selectors_require_discovery"]([welllog, "osdu:wks:*:*"]))
        self.assertEqual(naming.matches_kind_selector(welllog, "*:wks:work-product-component--Well*:1.*"), funcs["matches_kind_selector"](welllog, "*:wks:work-product-component--Well*:1.*"))

    def test_schema_helpers_match_notebook(self) -> None:
        funcs = extract_functions(
            [
                "_schema_definitions",
                "_definition_key_from_ref",
                "_first_non_null_json_type",
                "normalize_kind_ref",
                "_assign_nested_property",
            ]
        )
        raw = {"definitions": {"legacy": {}}, "$defs": {"modern": {}}}
        self.assertEqual(schema.schema_definitions(raw), funcs["_schema_definitions"](raw))
        self.assertEqual(schema.definition_key_from_ref("#/$defs/modern"), funcs["_definition_key_from_ref"]("#/$defs/modern"))
        self.assertEqual(schema.first_non_null_json_type(["null", "string"]), funcs["_first_non_null_json_type"](["null", "string"]))
        self.assertEqual(schema.normalize_kind_ref("{{schema-authority}}:{{wksNameSpace}}:x:{{wksVersion}}"), funcs["normalize_kind_ref"]("{{schema-authority}}:{{wksNameSpace}}:x:{{wksVersion}}"))

        module_properties: dict[str, Any] = {}
        notebook_properties: dict[str, Any] = {}
        schema.assign_nested_property(module_properties, ["A", "B"], {"type": "string"})
        funcs["_assign_nested_property"](notebook_properties, ["A", "B"], {"type": "string"})
        self.assertEqual(module_properties, notebook_properties)


if __name__ == "__main__":
    unittest.main()
