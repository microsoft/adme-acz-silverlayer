import ast
import hashlib
import importlib.util
import json
import os
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock
from urllib.parse import quote

try:
    from pyspark.sql import types as SparkTypes
except ImportError:  # pragma: no cover - local notebook helper tests can run without PySpark.
    SparkTypes = None


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


def top_level_assignment_value(nb: dict, name: str):
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        tree = ast.parse("".join(cell.get("source", [])))
        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                return ast.literal_eval(node.value)
    raise AssertionError(f"Assignment {name!r} was not found")


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
                namespace: dict[str, object] = {
                    "hashlib": hashlib,
                    "json": json,
                    "os": os,
                    "Any": Any,
                    "KindResult": object,
                    "MERGE_KEY_COLUMNS": ["id", "version"],
                    "_DELTA_COLUMN_PART_RE": re.compile(r"[^0-9A-Za-z_]+"),
                }
                if SparkTypes is not None:
                    namespace["T"] = SparkTypes
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
    namespace: dict[str, object] = {
        "hashlib": hashlib,
        "json": json,
        "os": os,
        "quote": quote,
        "re": re,
        "ADME_SCHEMA_RETRY_STATUS_CODES": [408, 429, 500, 502, 503, 504],
        "ADME_SCHEMA_SERVICE_PATH": "/api/schema-service/v1/schema",
        "ADME_TOKEN_SCOPE": "https://management.core.windows.net/.default",
        "ADME_DEVICE_CODE_CLIENT_ID": "04b07795-8ddb-461a-bbee-02f9e1bf7b46",
        "Any": Any,
        "_DATA_PREFIX": "data__",
        "_JSON_SCHEMA_MARKER_KEYS": {
            "$schema",
            "$id",
            "allOf",
            "anyOf",
            "definitions",
            "oneOf",
            "properties",
            "type",
            "x-osdu-schema-source",
        },
        "adme_endpoint": "https://contoso.energy.azure.com",
        "adme_data_partition_id": "data",
        "adme_auth_method": "SP",
        "adme_tenant_id": "11111111-1111-1111-1111-111111111111",
        "adme_sp_client_id": "22222222-2222-2222-2222-222222222222",
        "adme_sp_secret_kv_name": "contoso-kv",
        "adme_sp_secret_name": "adme-sp-secret",
        "KindResult": object,
        "DataFrame": object,
        "SparkSession": object,
        "MERGE_KEY_COLUMNS": ["id", "version"],
        "_DELTA_COLUMN_PART_RE": re.compile(r"[^0-9A-Za-z_]+"),
    }
    if SparkTypes is not None:
        namespace["T"] = SparkTypes
    exec(compile(module, filename="<notebook-functions>", mode="exec"), namespace)
    return {name: namespace[name] for name in function_names}


class TypedArrayExtractionTests(unittest.TestCase):
    """Exercise notebook extraction decisions with Spark execution boundaries mocked."""

    FEATURES = "data__SpatialLocation__AsIngestedCoordinates__features"
    CHILD = "osdu_wks_well___spatiallocation__asingestedcoordinates__features"

    def extract_features(self, values: list, create_empty: bool):
        class StructType:
            pass

        class StringType:
            pass

        class ArrayType:
            def __init__(self, element_type):
                self.elementType = element_type

        types = SimpleNamespace(
            DataType=object, StructType=StructType, StringType=StringType, ArrayType=ArrayType,
        )
        with mock.patch.dict(globals(), {"SparkTypes": types}):
            funcs = extract_functions(
                load_notebook(),
                [
                    "_extract_typed_arrays",
                    "_sanitize_table_name_part",
                    "_child_table_suffix",
                    "_child_table_prefix",
                    "child_table_name",
                    "_add_child_table",
                ],
            )

        parent = mock.Mock(spec=["schema", "columns", "drop"])
        element_type = StructType()
        parent.schema = SimpleNamespace(
            fields=[SimpleNamespace(name=self.FEATURES, dataType=ArrayType(element_type))],
        )
        parent.columns = ["id", "version", self.FEATURES]
        parent.drop.return_value = SimpleNamespace(columns=["id", "version"])
        child = object()
        explode = mock.Mock(return_value=child)
        extract = funcs["_extract_typed_arrays"]
        extract.__globals__.update(
            logger=mock.Mock(),
            create_empty_child_tables=create_empty,
            _batch_sample_string_arrays=mock.Mock(return_value={}),
            _batch_sample_array_presence=mock.Mock(
                return_value={self.FEATURES: any(value is not None and len(value) > 0 for value in values)},
            ),
            _explode_typed_array=explode,
        )
        children, projected, _ = extract(parent, "osdu_wks_well")
        return children, projected, parent, child, explode, element_type

    def test_empty_struct_arrays_are_removed_from_parent_and_honor_empty_child_setting(self) -> None:
        for values in ([None, None], [[], []], [None, []]):
            for create_empty in (True, False):
                with self.subTest(values=values, create_empty_child_tables=create_empty):
                    children, projected, _, child, _, _ = self.extract_features(values, create_empty)
                    expected_children = {self.CHILD: (self.FEATURES, child)} if create_empty else {}
                    self.assertEqual(
                        (projected.columns, children),
                        (["id", "version"], expected_children),
                        "Empty struct arrays must not remain on normalized parents; "
                        "CREATE_EMPTY_CHILD_TABLES controls only empty child publication.",
                    )

    def test_populated_struct_arrays_are_extracted_for_both_empty_child_settings(self) -> None:
        for create_empty in (True, False):
            with self.subTest(create_empty_child_tables=create_empty):
                children, projected, parent, child, explode, element_type = self.extract_features(
                    [None, [], [{"type": "Feature"}]], create_empty,
                )
                self.assertEqual(projected.columns, ["id", "version"])
                self.assertEqual(children, {self.CHILD: (self.FEATURES, child)})
                parent.drop.assert_called_once_with(self.FEATURES)
                explode.assert_called_once_with(parent, self.FEATURES, element_type, schema_cache=None)


class NotebookSimplificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.nb = load_notebook()

    def test_storage_probe_failures_are_not_cached_as_missing_tables(self) -> None:
        exists = extract_functions(self.nb, ["_table_exists"])["_table_exists"]
        cache = {}
        exists.__globals__.update({
            "_TABLE_EXISTS_CACHE": cache,
            "_requires_path_fallback": lambda error: False,
            "_table_path_uri": lambda table: f"file:///unreadable-fixture/{table}",
        })
        spark = mock.Mock()
        spark.catalog.tableExists.return_value = False
        filesystem = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
        filesystem.exists.return_value = False
        delta_table = mock.Mock()
        modules = {"delta": SimpleNamespace(), "delta.tables": SimpleNamespace(DeltaTable=delta_table)}
        with mock.patch.dict(sys.modules, modules):
            for failure in (PermissionError("Denied"), OSError("Storage unavailable")):
                with self.subTest(failure=type(failure).__name__):
                    delta_table.isDeltaTable.side_effect = failure
                    with self.assertRaises((PermissionError, OSError, RuntimeError)):
                        exists(spark, "owned_table", refresh=True)
                    self.assertNotIn("owned_table", cache)
            delta_table.isDeltaTable.side_effect = None
            delta_table.isDeltaTable.return_value = False
            self.assertFalse(exists(spark, "absent_table", refresh=True))
            self.assertIs(cache["absent_table"], False)
            filesystem.exists.return_value = True
            with self.assertRaisesRegex(RuntimeError, "refusing initialization"):
                exists(spark, "non_delta_table", refresh=True)
            self.assertNotIn("non_delta_table", cache)
            filesystem.exists.side_effect = PermissionError("Filesystem probe denied")
            with self.assertRaisesRegex(RuntimeError, "refusing initialization"):
                exists(spark, "inaccessible_table", refresh=True)
            self.assertNotIn("inaccessible_table", cache)
            filesystem.exists.side_effect = None
            filesystem.exists.return_value = False
            delta_table.isDeltaTable.return_value = True
            self.assertTrue(exists(spark, "owned_table", refresh=True))
            self.assertIs(cache["owned_table"], True)

    def test_writer_guard_fails_closed_on_lost_acquisition_or_invalid_singleton(self) -> None:
        acquire = extract_functions(self.nb, ["_acquire_writer"])["_acquire_writer"]
        guard = mock.Mock()
        guard.toDF.return_value.limit.return_value = guard.toDF.return_value
        acquire.__globals__.update({
            "_WRITER_GUARD_TABLE": "silver_writer_guard",
            "_table_exists": lambda *args, **kwargs: True,
            "_delta_table_for_target": lambda *args: guard,
            "F": mock.MagicMock(),
        })
        for rows, error in (
            ([[], []], "singleton"),
            ([[{"slot": 1, "owner_id": None}, {"slot": 1, "owner_id": None}], []], "singleton"),
            ([[{"slot": 1, "owner_id": None}], [{"slot": 1, "owner_id": "other-writer"}]], "ownership"),
        ):
            with self.subTest(rows=rows):
                guard.reset_mock()
                guard.toDF.return_value.collect.side_effect = rows
                with self.assertRaisesRegex(RuntimeError, error):
                    acquire(mock.Mock(), "requesting-writer")
                if error == "singleton":
                    guard.update.assert_not_called()
                else:
                    guard.update.assert_called_once()

    def test_failed_group_does_not_publish_its_preflight_bridge_tables(self) -> None:
        build = extract_functions(self.nb, ["_run_silver_build_attempt"])["_run_silver_build_attempt"]
        namespace = build.__globals__
        kinds = ["example:wks:master-data--Failed:1.0.0", "example:wks:master-data--Ready:1.0.0"]
        tables = {kinds[0]: "failed__field__target", kinds[1]: "ready__field__target"}
        groups = [{"group_key": kind, "kinds": [kind]} for kind in kinds]
        frame = mock.MagicMock()
        frame.columns = ["id", "version", "kind", "isActive"]
        frame.select.return_value = frame
        frame.persist.return_value = frame
        frame.where.return_value = frame
        frame.drop.return_value = frame
        writes = []
        recorded_results = []

        def result(**values):
            item = SimpleNamespace(**{
                "records_processed": 0, "records_failed": 0, "reassembled": False,
                "child_tables": [], "error": None, "validation_passed": True, **values,
            })
            recorded_results.append(item)
            return item

        def process(_spark, group, *args, **kwargs):
            kind = group["group_key"]
            if kind == kinds[0]:
                raise ValueError("Injected group failure")
            kwargs["relationship_frames"].append(frame)
            kwargs["relationship_bridge_tables"].append(tables[kind])
            return [result(kind=kind, status="success", parent_table="ready", child_tables=[tables[kind]])]

        namespace.update({
            "perf_counter": lambda: 0, "datetime": datetime, "UTC": timezone.utc,
            "uuid": SimpleNamespace(uuid4=lambda: "fixture-run"), "logger": mock.Mock(),
            "traceback": mock.Mock(), "KindResult": result, "F": mock.MagicMock(),
            "_effective_merge_key_columns": lambda value: value or ["id", "version"],
            "_watermark_active": lambda *args: False, "_processing_limits_active": lambda *args: False,
            "_include_inactive_records": lambda: False, "prepare_bronze_df": lambda *args, **kwargs: (frame, False),
            "_normalize_bronze_record_wrapper": lambda *args: frame,
            "_validate_record_identities": mock.Mock(),
            "_validate_existing_bridges": mock.Mock(),
            "_validated_historical_outputs": lambda *args: [],
            "apply_active_record_filter": lambda *args, **kwargs: frame,
            "_resolve_owned_kind_selectors": lambda *args, **kwargs: kinds,
            "_persist_publication": mock.Mock(), "_record_output_ownership": mock.Mock(),
            "group_kinds_by_version_strategy": lambda *args: groups,
            "validate_adme_schema_service_access": lambda: "synthetic",
            "validate_build_plan_or_raise": lambda *args: ["failed", "ready"],
            "_assert_overwrite_allowed": mock.Mock(), "write_run_status": mock.Mock(),
            "preflight_kind_counts": False, "prefetch_schema_registry": lambda *args, **kwargs: (mock.Mock(), {}),
            "relationship_bridge_tables_for_kind": lambda kind, *args: [tables[kind]],
            "_metadata_table_names": lambda *args: [], "validate_table_names": mock.Mock(),
            "read_bronze_table_spark": lambda *args, **kwargs: frame,
            "_storage_level_from_name": lambda *args: None, "process_kind_group": process,
            "table_name_for_kind_group": lambda group, prefix: group["group_key"],
            "run_info_row": lambda *args: (), "run_manifest_row": lambda *args: (),
            "_union_frames": lambda frames: frame,
            "write_silver_tables": lambda batch, **kwargs: writes.extend(target for _, target in batch),
            "_write_scoped_refresh": lambda spark, batch, *args: writes.extend(target for _, target in batch),
            "_buffer_or_write_output_documentation": mock.Mock(),
            "write_incremental_watermark_state": mock.Mock(),
            "flush_metadata_buffers": lambda spark, info, manifest, *args: (info.clear(), manifest.clear()),
            "_output_tables_from_results": lambda *args: [],
            "print": lambda *args, **kwargs: None,
        })
        with self.assertRaisesRegex(RuntimeError, "kind group.*failed") as failure:
            build(
                mock.MagicMock(), kinds, "workspace", "lakehouse", bronze_table="bronze", allow_overwrite=True,
                publication={"run_id": "fixture-run", "context_id": "fixture-context",
                             "source_version": 0, "source_path": "file:///fixture-bronze"},
            )
        self.assertIn("Attempt fixture-run", str(failure.exception))
        self.assertIn(f"{kinds[0]}: Injected group failure", str(failure.exception))
        self.assertEqual(namespace["write_run_status"].call_args.kwargs["error_message"], str(failure.exception))
        self.assertEqual([item.status for item in recorded_results], ["failed", "success"])
        self.assertIn("Injected group failure", recorded_results[0].error)
        self.assertEqual(writes, ["ready__field__target"])
        namespace["_validate_record_identities"].assert_called_once_with(frame)
        namespace["_validate_existing_bridges"].assert_any_call(mock.ANY, list(tables.values()))

    def test_historical_ownership_reports_rejected_tables_without_accepting_them(self) -> None:
        funcs = extract_functions(self.nb, [
            "_validated_historical_outputs", "_relationship_table_prefix", "_bounded_relationship_name",
            "_is_relationship_bridge", "validate_table_names", "_is_array_child", "_child_table_prefix",
        ])
        validate = funcs["_validated_historical_outputs"]
        validate.__globals__["_TABLE_NAME_PATTERN"] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
        for strategy in ("merge", "versioned_tables"):
            parent = "osdu_wks_well" + ("__v1_0_0" if strategy == "versioned_tables" else "")
            bridge_prefix = funcs["_relationship_table_prefix"](parent)
            allowed = [parent + "___namealiases", bridge_prefix + "facilitytypeid__reference_data_facilitytype"]
            with self.subTest(strategy=strategy):
                validate.__globals__["_manifest_child_tables_for_parent"] = lambda *args: allowed
                self.assertEqual(allowed, validate(None, parent, "workspace", "lakehouse"))
                rejected = [f"unrelated_table_{i}" for i in range(12)]
                validate.__globals__["_manifest_child_tables_for_parent"] = lambda *args: allowed + rejected
                with self.assertRaisesRegex(RuntimeError, "ownership namespace") as failure:
                    validate(None, parent, "workspace", "lakehouse")
                message = str(failure.exception)
                self.assertIn("12 rejected table(s)", message)
                self.assertIn(repr(rejected[:10]), message)
                self.assertNotIn("unrelated_table_10", message)
                self.assertIn(bridge_prefix, message)
                self.assertIn("silver_run_manifest", message)
                self.assertIn("silver_output_ownership", message)

    @unittest.skipUnless(importlib.util.find_spec("pandas"), "Install pandas for results display tests.")
    def test_results_summary_separates_and_deduplicates_bridges(self) -> None:
        source = "".join(self.nb["cells"][-1]["source"])
        bridge = "fixture_well__facility__facilitytype"
        helpers = extract_functions(self.nb, [
            "_is_relationship_bridge", "_relationship_table_prefix", "_bounded_relationship_name",
        ])
        for wide in (False, True):
            with self.subTest(wide=wide):
                displayed = []
                results = [
                    SimpleNamespace(
                        kind=f"osdu:wks:master-data--Well:{version}",
                        status="success", records_processed=2, parent_table="fixture_well",
                        child_tables=[bridge, bridge] if wide else ["fixture_well___aliases", bridge, bridge],
                        reassembled=wide, validation_passed=True, error=None,
                    )
                    for version in ("1.0.0", "2.0.0")
                ]
                namespace = {
                    "results": results, "table_prefix": "fixture_",
                    "display": displayed.append, "print": lambda *args: None,
                    "_is_relationship_bridge": helpers["_is_relationship_bridge"],
                }
                exec(compile(source, "<results-summary>", "exec"), namespace)
                self.assertEqual(len(displayed), 2)
                self.assertEqual(displayed[0]["Children"].tolist(), ["wide", "wide"] if wide else [1, 1])
                self.assertEqual(displayed[0]["Bridges"].tolist(), [1, 1])
                self.assertEqual(displayed[1].to_dict("records"), [{
                    "Bridge table": bridge,
                    "Source kinds": ", ".join(r.kind for r in results),
                    "Parent tables": "fixture_well",
                    "Source statuses": "success",
                }])

    @unittest.skipUnless(importlib.util.find_spec("pandas"), "Install pandas for results display tests.")
    def test_results_summary_without_bridges_and_without_execution(self) -> None:
        source = "".join(self.nb["cells"][-1]["source"])
        result = SimpleNamespace(
            kind="osdu:wks:master-data--Well:1.0.0",
            status="skipped", records_processed=0, parent_table="well",
            child_tables=None, reassembled=False, validation_passed=True, error=None,
        )
        for results, dry_run in (([result], []), ([], [{"kind": result.kind}]), ([], [])):
            with self.subTest(results=bool(results), dry_run=bool(dry_run)):
                displayed = []
                namespace = {
                    "results": results, "table_prefix": "", "dry_run_results": dry_run,
                    "display": displayed.append, "print": lambda *args: None,
                }
                exec(compile(source, "<results-summary>", "exec"), namespace)
                self.assertEqual(len(displayed), 1 if results or dry_run else 0)
                self.assertEqual(namespace["bridge_inventory"], {})
                if results:
                    self.assertEqual(displayed[0]["Bridges"].tolist(), [0])
                elif dry_run:
                    self.assertEqual(displayed[0].to_dict("records"), dry_run)

    def test_notebook_structure_is_valid(self) -> None:
        self.assertEqual(self.nb["nbformat"], 4)
        self.assertGreaterEqual(len(self.nb["cells"]), 20)
        self.assertTrue(all(cell["cell_type"] in {"markdown", "code"} for cell in self.nb["cells"]))
        self.assertTrue(all(not cell.get("outputs") for cell in self.nb["cells"] if cell["cell_type"] == "code"))

    def test_implementation_only_cells_are_hidden(self) -> None:
        utility_sections = {
            "## Pipeline constants",
            "## Helper functions",
            "### Schema service, authentication, and cache helpers",
            "### Schema registry and child table naming helpers",
            "### Delta column sanitization and schema inference helpers",
            "### Envelope extraction, column classification, and child builders",
            "### JSON flattening and kind decomposition",
            "### Wide reassembly and output documentation schema helpers",
            "### Delta table safety, incremental state, and inactive deletes",
            "### Data-quality and output-documentation write helpers",
            "### Single-kind processing",
            "### Kind-group processing",
            "### Kind grouping, metadata rows, and build-plan validation",
            "### Kind selector resolution and output preview",
            "### Setup checklist implementation",
            "### Dry-run implementation",
            "### Pipeline execution orchestration",
        }
        section = None
        hidden_cells = []
        for cell in self.nb["cells"]:
            if cell["cell_type"] == "markdown":
                headings = [
                    line.strip()
                    for line in "".join(cell.get("source", [])).splitlines()
                    if line.startswith("#")
                ]
                if headings:
                    section = headings[0]
                continue
            if cell["cell_type"] != "code":
                continue
            metadata = cell.get("metadata", {})
            runtime_resolution = "FABRIC RUNTIME RESOLUTION" in "".join(cell.get("source", []))
            if section in utility_sections or runtime_resolution:
                self.assertTrue(metadata.get("collapsed"))
                self.assertTrue(metadata.get("jupyter", {}).get("source_hidden"))
                if runtime_resolution:
                    self.assertFalse(metadata.get("jupyter", {}).get("outputs_hidden"))
                else:
                    hidden_cells.append(section)
            else:
                self.assertFalse(metadata.get("jupyter", {}).get("source_hidden"))

        self.assertEqual(set(hidden_cells), utility_sections)

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
        self.assertIn('RUN_PROFILE = "inspect"', source)
        self.assertIn('"dry_run"', source)

    def test_run_profile_values_are_explicit(self) -> None:
        source = notebook_source(self.nb, "code")
        self.assertIn('{"inspect", "dry_run", "execute"}', source)
        self.assertIn(
            'raise ValueError("RUN_PROFILE must be \'inspect\', \'dry_run\', or \'execute\'.")',
            source,
        )
        self.assertIn('elif run_profile == "execute":', source)
        self.assertNotIn('run_profile == "interactive"', source)
        self.assertNotIn('run_profile == "full"', source)
        self.assertIn("# Run stage. Start with inspect, then dry_run, then execute.", source)
        markdown = notebook_source(self.nb, "markdown")
        self.assertIn("`inspect`: print effective settings and next steps", markdown)
        self.assertIn("`execute`: run the configured pipeline.", markdown)
        self.assertNotIn("`interactive`: print current settings", markdown)
        self.assertNotIn("`full`: process the configured kinds", markdown)
        self.assertNotIn("full pipeline", markdown.lower())
        self.assertNotIn("full execution", notebook_source(self.nb).lower())

    def test_delta_column_name_sanitizer_handles_schema_placeholders(self) -> None:
        funcs = extract_functions(
            self.nb,
            ["_sanitize_column_name_part", "sanitize_delta_column_name", "make_delta_column_alias"],
        )
        sanitize = funcs["sanitize_delta_column_name"]
        alias_for = funcs["make_delta_column_alias"]

        self.assertEqual(sanitize("data__(COMPANY: insert comment)"), "data__COMPANY_insert_comment")
        self.assertEqual(sanitize("123 bad=field"), "field_123_bad_field")

        used: set[str] = set()
        self.assertEqual(alias_for("data__(COMPANY: insert comment)", used), "data__COMPANY_insert_comment")
        collision = alias_for("data__COMPANY insert comment", used)
        self.assertRegex(collision, r"^data__COMPANY_insert_comment_[0-9a-f]{8}$")

        source = notebook_source(self.nb, "code")
        self.assertIn("assert_delta_safe_column_names(df", source)
        self.assertIn("_record_stage(\"sanitize_delta_columns\"", source)

    def test_explicit_tenant_config_and_repeatability_controls_are_present(self) -> None:
        source = notebook_source(self.nb, "code")
        for expected in [
            'WORKSPACE_ID = ""',
            'LAKEHOUSE_ID = ""',
            'BRONZE_TABLE = "osducatalog"',
            'ADME_ENDPOINT = ""',
            'ADME_DATA_PARTITION_ID = ""',
            'ADME_AUTH_METHOD = "SP"',
            'ADME_MANAGED_IDENTITY_CLIENT_ID = ""',
            'ADME_TENANT_ID = ""',
            'ADME_SP_CLIENT_ID = ""',
            'ADME_SP_SECRET_KV_NAME = ""',
            'ADME_SP_SECRET_NAME = ""',
            'ADME_TOKEN_SCOPE = "https://management.core.windows.net/.default"',
            'ADME_DEVICE_CODE_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"',
            '_parameters = resolve_notebook_parameters(_parameter_settings, dict(os.environ))',
            'NOTEBOOK_VERSION = "0.5.6"',
            "ALLOW_OVERWRITE = False",
            "WRITE_RELATIONSHIP_BRIDGES = True",
            'write_relationship_bridges = _parameters["WRITE_RELATIONSHIP_BRIDGES"]',
            'MERGE_KEY_COLUMNS = ["id", "version"]',
            "ADME_MERGE_KEY_COLUMNS",
            'allow_overwrite = _parameters["ALLOW_OVERWRITE"]',
            'WRITE_MODE = "upsert"',
            "ADME_WRITE_MODE",
            "ADME_INCREMENTAL",
            'INCREMENTAL_WATERMARK_COLUMN = "ingestTime"',
            'INCREMENTAL_WATERMARK_MODE = "auto"',
            'INCREMENTAL_STATE_TABLE = "silver_incremental_state"',
            "RETRY_SKIPPED_SCHEMA_RECORDS = True",
            "config_hash = hashlib.sha256",
            "PERSIST_SCHEMA_CACHE = True",
            'SCHEMA_CACHE_TABLE = "silver_schema_cache"',
            'RUN_MANIFEST_TABLE = "silver_run_manifest"',
            'RUN_STATUS_TABLE = "silver_run_status"',
            'schema_cache_writes_enabled = persist_schema_cache and run_profile == "execute"',
            "KIND_LIMITS = {}",
            "ADME_KIND_LIMITS",
            "kind_selectors",
            "EXCLUDED_KINDS = []",
            "excluded_kind_selectors",
            "WRITE_OUTPUT_DOCS = True",
            'OUTPUT_DOCS_TABLE = "silver_output_documentation"',
            "DATA_QUALITY_CHECKS = True",
            'DATA_QUALITY_ISSUES_TABLE = "silver_data_quality_issues"',
            "DATA_QUALITY_MAX_EXAMPLES = 100",
            'VERSION_STRATEGY = "versioned_tables"',
            'MISSING_SCHEMA_MODE = "skip"',
            "CREATE_EMPTY_CHILD_TABLES = True",
            "CACHE_BRONZE = True",
            'BRONZE_CACHE_STORAGE_LEVEL = "MEMORY_AND_DISK"',
            "PREFLIGHT_KIND_COUNTS = True",
            "BATCH_METADATA_WRITES = True",
            "METADATA_FLUSH_INTERVAL = 100",
            'OUTPUT_DOCS_MODE = "summary"',
            "SCHEMA_PREFLIGHT = True",
            "SCHEMA_FETCH_PARALLELISM = 4",
            'OUTPUT_WRITE_PARALLELISM = "auto"',
            "WIDE_MAX_CARDINALITY_CAP = 20",
            "ADME_SCHEMA_TIMEOUT_SECONDS = 30",
            "ADME_SCHEMA_RETRY_TOTAL = 3",
            "ADME_SCHEMA_RETRY_BACKOFF_SECONDS = 1.0",
            "ADME_SCHEMA_RETRY_STATUS_CODES = [408, 429, 500, 502, 503, 504]",
        ]:
            self.assertIn(expected, source)
        self.assertNotIn("INCREMENTAL = False", source)

    def test_active_record_filter_controls_are_present(self) -> None:
        source = notebook_source(self.nb, "code")
        for expected in [
            "INCLUDE_INACTIVE_RECORDS = False",
            'include_inactive_records = _parameters["INCLUDE_INACTIVE_RECORDS"]',
            '"include_inactive_records": include_inactive_records',
            "def active_record_filter_status(",
            "def apply_active_record_filter(",
            'if "isActive" not in df.columns:',
            'F.col("isActive") == F.lit(True)',
            "apply_active_filter: bool = True",
            "return apply_active_record_filter(df) if apply_active_filter else df",
            'T.StructField("include_inactive_records", T.BooleanType(), True)',
            'bool(globals().get("include_inactive_records", False))',
            '_check_row("active record filter"',
        ]:
            self.assertIn(expected, source)

    def test_customer_settings_are_separate_from_configuration_code(self) -> None:
        code_cells = [cell for cell in self.nb["cells"] if cell["cell_type"] == "code"]
        config_index = next(
            index for index, cell in enumerate(code_cells)
            if 'WORKSPACE_ID = ""' in "".join(cell["source"])
        )
        config_source = "".join(code_cells[config_index]["source"])
        runtime_source = "".join(code_cells[config_index + 1]["source"])

        self.assertNotIn("import ", config_source)
        self.assertNotIn("def ", config_source)
        self.assertIn("def _resolve_workspace_id(", runtime_source)
        self.assertNotIn('WORKSPACE_ID = ""', runtime_source)

    def test_relationship_bridge_is_part_of_the_pipeline_output_contract(self) -> None:
        source = notebook_source(self.nb, "code")

        self.assertIn("def relationship_bridge_table_name(", source)
        self.assertIn('alias("_relationship_bridge_table")', source)
        self.assertIn("def resolve_direct_relationships(", source)
        self.assertIn("relationship_frames=relationship_frames", source)
        self.assertIn("relationship_changed_key_frames=relationship_changed_key_frames", source)
        self.assertIn("relationship_bridge_tables=relationship_bridge_tables", source)
        self.assertIn('relationship_merge_keys = [f"source_{key}" for key in merge_keys]', source)
        self.assertNotIn("__fk_id", source)
        self.assertNotIn("__fk_version", source)

    def test_relationship_bridge_flag_controls_resolution_and_is_reported(self) -> None:
        source = notebook_source(self.nb, "code")
        for expected in [
            '"write_relationship_bridges": write_relationship_bridges',
            "if write_relationship_bridges and registry.direct_relationship_fields(kind):",
            "if schema_registry and write_relationship_bridges:",
            'row["relationship_bridges_enabled"] and row["schema_resolved"]',
            'T.StructField("relationship_bridges_enabled", T.BooleanType(), True)',
            "relationship_bridges_enabled,",
            "write_relationship_bridges=write_relationship_bridges",
        ]:
            self.assertIn(expected, source)

    def test_run_manifest_records_disabled_relationship_bridges(self) -> None:
        run_manifest_row = extract_function(self.nb, "run_manifest_row")

        class Clock:
            @staticmethod
            def now(_timezone):
                return "created-at"

        run_manifest_row.__globals__.update({
            "UTC": None,
            "datetime": Clock,
            "kind_family_key": lambda kind: kind,
            "kind_version": lambda kind: "1.0.0",
            "version_strategy": "merge",
            "output_docs_mode": "summary",
            "persist_schema_cache": True,
            "cache_bronze": True,
            "include_inactive_records": False,
        })
        result = SimpleNamespace(
            kind="osdu:wks:master-data--Well:1.0.0",
            child_tables=[],
            schema_mode="resolved",
            parent_table="well",
            records_processed=1,
            status="success",
            error=None,
            quality_status="passed",
            quality_issue_count=0,
        )
        row = run_manifest_row(
            "run-id", result, "normalized", "", "osducatalog", "execute", "0.5.6",
            "config-hash", False, "upsert", ["id", "version"], None, "off", False,
        )

        self.assertFalse(row[-1])

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
            "def _clean_optional_kind_selectors(",
            "def _filter_excluded_kinds(",
            "def discover_bronze_kinds(",
            "def resolve_kind_selectors(",
            "def resolve_affected_kind_selectors(",
            "def ensure_resolved_kinds(",
            "def read_bronze_table_spark(",
            "def _normalize_bronze_record_wrapper(",
            'F.get_json_object(F.col("data"), "$.data")',
            'F.get_json_object(F.col("data"), "$.createUser")',
            'F.get_json_object(F.col("data"), "$.createTime")',
            "df = _normalize_bronze_record_wrapper(df)",
            "def prepare_bronze_df(",
            "def write_silver_tables(",
            "def apply_inactive_record_filter(",
            "def _merge_condition(",
            "def _assert_no_duplicate_merge_keys(",
            "def _validate_merge_key_columns(",
            "def _effective_merge_key_columns(",
            "def _align_source_to_target_schema(",
            "def _read_target_df_for_merge(",
            "def _reassemble_key_columns(",
            "def compute_kind_counts(",
            "def prefetch_schema_registry(",
            "def flush_metadata_buffers(",
            "INCREMENTAL_STATE_SCHEMA",
            "def load_incremental_watermark_state(",
            "table_name = _metadata_table_name(workspace_id, lakehouse_id, logical_table_name) if workspace_id or lakehouse_id else logical_table_name",
            "load_incremental_watermark_state(spark, None, watermark_column, workspace_id, lakehouse_id, context_id=context_id)",
            "def load_schema_retry_kinds(",
            "def apply_incremental_watermark_filter(",
            "def apply_incremental_watermark_filter_all_kinds(",
            "def write_incremental_watermark_state(",
            "def delete_inactive_silver_rows(",
            "def flush_output_documentation_rows(",
            "def load_schema_docs(",
            "def run_info_row(",
            "def run_manifest_row(",
            "def kind_family_key(",
            "def kind_to_versioned_table_name(",
            "def group_kinds_by_version_strategy(",
            "def detect_table_collisions(",
            "def process_kind_group(",
            "def _schema_definitions(",
            "def _definition_key_from_ref(",
            "def _first_non_null_json_type(",
            "def _dedupe_case_insensitive_struct_type(",
            "def infer_schema_doc_from_bronze_df(",
            "def build_inferred_registry_from_bronze(",
            "def _fetch_schema_docs_from_adme(",
            "def _schema_fetch_parallelism(",
            "def _output_write_parallelism_for_sku(",
            "def _wide_max_cardinality_cap(",
            "schema = _merge_schemas(schema, inferred)",
            '"x-osdu-schema-source": "inferred-from-bronze"',
            'schema_mode = "inferred"',
            "ConfidentialClientApplication",
            "PublicClientApplication",
            "def _adme_keyvault_url(",
            "def _adme_service_principal_secret(",
            "def get_adme_access_token(",
            "def _adme_schema_url(",
            "def _adme_schema_list_url(",
            "def _adme_schema_headers(",
            "def validate_adme_schema_service_access(",
            "def build_registry_from_adme(",
            "def _load_schema_docs_from_persistent_cache(",
            "def _write_schema_docs_to_persistent_cache(",
            "validate_adme_schema_service_access()",
            'timings["schema_access_check"]',
            "def write_run_status(",
            "def _output_tables_from_results(",
            "def collect_data_quality_issues(",
            "def evaluate_and_write_data_quality_issues(",
            "def write_data_quality_issues(",
            "def flush_data_quality_issue_rows(",
            "def output_documentation_rows(",
            "def _buffer_or_write_output_documentation(",
            "def validate_table_names(",
            "def _assert_overwrite_allowed(",
            "RUN_MANIFEST_SCHEMA",
            "RUN_STATUS_SCHEMA",
            "SCHEMA_CACHE_SCHEMA",
            "DATA_QUALITY_ISSUES_SCHEMA",
            "OUTPUT_DOCS_SCHEMA",
            'T.StructField("notebook_version", T.StringType(), True)',
            'T.StructField("config_hash", T.StringType(), True)',
            'T.StructField("allow_overwrite", T.BooleanType(), True)',
            'T.StructField("duration_seconds", T.DoubleType(), True)',
            'T.StructField("error_type", T.StringType(), True)',
            'T.StructField("stage_timings_json", T.StringType(), True)',
            'T.StructField("watermark_column", T.StringType(), True)',
            'T.StructField("watermark_value", T.StringType(), True)',
            'T.StructField("quality_status", T.StringType(), True)',
            'T.StructField("quality_issue_count", T.LongType(), True)',
            'T.StructField("table_role", T.StringType(), False)',
            'T.StructField("column_name", T.StringType(), False)',
            'T.StructField("version_strategy", T.StringType(), True)',
            'T.StructField("schema_versions", T.ArrayType(T.StringType()), True)',
            'T.StructField("schema_mode", T.StringType(), True)',
            "silver_output_documentation",
            "silver_data_quality_issues",
            "Timing summary",
            "metadata_flush",
            "data_quality_flush",
            "output_docs_flush",
            "silver_run_status",
            'final_status = "failed" if failed else "committed"',
            "outputs are not marked committed",
            "ADME schema retries",
            "filters source writes; enabled bridges reconcile all selected published sources against globally observed targets",
            "explicit inactive rows hard-delete Silver keys",
            "inactive rows are included by configuration",
            "inactive_delete_preview_rows",
            "No affected bronze rows after ingestTime watermark filtering",
            "inactive_bronze_df=inactive_bronze_df",
            "affected_kind_bronze_df = active_bronze_df.unionByName(inactive_bronze_df, allowMissingColumns=True)",
            'schema_access_detail = "not checked"',
            "affected_bronze_df = apply_incremental_watermark_filter_all_kinds(",
            "schema_retry_kinds = load_schema_retry_kinds(",
            "schema_retry_kinds=schema_retry_kinds",
            "Retrying prior schema-missing kind",
            "Schema retry kinds:",
            "if not _retry_skipped_schema_records() and _watermark_active",
            "write_incremental_watermark_state(spark, watermark_updates, workspace_id, lakehouse_id)",
            'timings["changed_bronze_cache"]',
            "Changed Bronze slice cached",
        ]:
            self.assertIn(expected, source)

    def test_incremental_run_prunes_changed_rows_before_schema_access(self) -> None:
        source = notebook_source(self.nb, "code")

        retry_kinds_index = source.index("schema_retry_kinds = load_schema_retry_kinds(")
        changed_slice_index = source.index("affected_bronze_df = apply_incremental_watermark_filter_all_kinds(")
        kind_pruning_index = source.index("kinds = resolve_affected_kind_selectors(", changed_slice_index)
        schema_access_index = source.index("schema_access_detail = validate_adme_schema_service_access()")
        schema_preflight_index = source.index("schema_registry, schema_status = prefetch_schema_registry(kinds, enabled=True)")

        self.assertLess(retry_kinds_index, changed_slice_index)
        self.assertLess(changed_slice_index, kind_pruning_index)
        self.assertLess(kind_pruning_index, schema_access_index)
        self.assertLess(schema_access_index, schema_preflight_index)
        self.assertIn("if write_relationship_bridges:\n                kinds = selected_kinds", source)

    def test_performance_resilience_controls_are_present(self) -> None:
        source = notebook_source(self.nb, "code")
        self.assertIn("flush_metadata_buffers(spark, run_info_rows, manifest_rows, workspace_id, lakehouse_id)", source)
        self.assertIn("flush_output_documentation_rows(spark, output_docs_rows, workspace_id, lakehouse_id)", source)
        self.assertIn("finally:", source)
        self.assertIn("bronze_df.unpersist()", source)
        self.assertIn("_table_exists(spark, name, refresh=True)", source)
        self.assertIn("_TABLE_EXISTS_CACHE[target] = True", source)
        self.assertIn('docs_mode == "summary"', source)
        self.assertIn('timings["output_docs_flush"]', source)
        self.assertIn('if children and not globals().get("create_empty_child_tables", True):', source)
        self.assertIn("_write_schema_docs_to_persistent_cache(cache_rows)", source)
        self.assertIn("HTTPAdapter(max_retries=retry)", source)
        self.assertIn("Retry(", source)
        self.assertIn("status_forcelist=_adme_schema_retry_status_codes()", source)
        self.assertIn("ThreadPoolExecutor(max_workers=parallelism)", source)
        self.assertIn("as_completed(future_by_kind)", source)
        self.assertIn("parallelism=%d", source)
        self.assertIn("Output write parallelism:", source)
        self.assertIn("Fabric SKU:", source)
        self.assertIn("import sempy.fabric as fabric", source)
        self.assertIn("fabric.get_sku_size(workspace=workspace_id or None)", source)
        self.assertIn("Writing %d Silver table(s) with output_write_parallelism=%d", source)
        self.assertIn("future_by_target = {executor.submit(write_silver_table, df, target, mode, **options): target for df, target in writes}", source)
        self.assertIn("quality_issue_rows.extend(rows)", source)
        self.assertIn("flush_data_quality_issue_rows(spark, data_quality_issue_rows, workspace_id, lakehouse_id)", source)
        self.assertIn("limits_active = _processing_limits_active(limit, kind_limits)", source)
        self.assertIn('if bool(globals().get("cache_bronze", True)) and not limits_active:', source)
        self.assertIn('.option("versionAsOf", publication["source_version"]).load(publication["source_path"])', source)
        self.assertIn('not watermark_filter_active and not limits_active', source)
        self.assertIn("Skipping kind count preflight for limited run", source)
        self.assertIn("effective_cardinality_cap = max_cardinality_cap or _wide_max_cardinality_cap()", source)
        self.assertIn("_adme_schema_get_json(list_url", source)
        self.assertIn("_adme_schema_get_json(schema_url", source)
        self.assertIn("def validate_incremental_limit_safety(", source)
        self.assertIn(">= F.lit(previous).cast(data_type)", source)
        self.assertIn(">= F.col(\"__previous_watermark\").cast(data_type)", source)
        self.assertIn("apply_active_filter=False", source)
        self.assertIn("bronze_df = apply_active_record_filter(pinned_bronze_df)", source)
        self.assertIn("include_inactive_records=False", source)
        self.assertIn('F.lower(F.col("isActive").cast("string")).isin("false", "0")', source)
        self.assertIn("if not successful_kinds and inactive_keys_df is None:", source)
        self.assertIn('schema_mode="inactive_delete"', source)
        self.assertIn("def validate_build_plan_or_raise(", source)
        self.assertIn("required_flush_errors.append(message)", source)
        self.assertIn("Required post-write finalization failed", source)
        self.assertNotIn("requests.get(", source)

    def test_full_run_receives_overwrite_and_metadata_arguments(self) -> None:
        source = notebook_source(self.nb, "code")
        self.assertIn("allow_overwrite=allow_overwrite", source)
        self.assertIn("notebook_version=NOTEBOOK_VERSION", source)
        self.assertIn("config_hash=config_hash", source)
        self.assertIn("Full refresh would overwrite existing table(s)", source)
        self.assertIn("Set ALLOW_OVERWRITE = True", source)
        self.assertIn("merge_key_columns=merge_key_columns", source)
        self.assertIn("changed_keys_df", source)
        self.assertIn("_merge_condition(\"target\", \"changed\", merge_key_columns)", source)
        self.assertIn("def _execute_delta_merge(", source)
        self.assertIn("not overwriting target table", source)
        self.assertIn("refusing to overwrite", source)
        self.assertNotIn("except Exception:\n        _write_table(df, target, mode=\"overwrite\")", source)
        self.assertIn("_align_source_to_target_schema(source_df, target_df)", source)
        self.assertIn("parent.join(pivoted, on=key_cols", source)
        self.assertIn("parent.join(agg_df, on=key_cols", source)

    def test_no_hardcoded_environment_ids_or_driver_collected_changed_ids(self) -> None:
        raw = NOTEBOOK.read_text(encoding="utf-8")
        raw = raw.replace("04b07795-8ddb-461a-bbee-02f9e1bf7b46", "")
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

    def test_write_mode_and_retry_parsers(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "_normalize_write_mode",
                "_normalize_watermark_mode",
                "_parse_retry_status_codes",
            ],
        )
        normalize_write_mode = funcs["_normalize_write_mode"]
        normalize_watermark_mode = funcs["_normalize_watermark_mode"]
        parse_retry_status_codes = funcs["_parse_retry_status_codes"]

        self.assertEqual(normalize_write_mode("", False), "full_refresh")
        self.assertEqual(normalize_write_mode("", True), "upsert")
        self.assertEqual(normalize_write_mode("incremental", False), "upsert")
        self.assertEqual(normalize_write_mode("overwrite", True), "full_refresh")
        with self.assertRaises(ValueError):
            normalize_write_mode("append", False)

        self.assertEqual(normalize_watermark_mode(None), "auto")
        self.assertEqual(normalize_watermark_mode("required"), "required")
        with self.assertRaises(ValueError):
            normalize_watermark_mode("strict")

        self.assertEqual(parse_retry_status_codes("429,500,503"), [429, 500, 503])
        self.assertEqual(parse_retry_status_codes("[408, 429]"), [408, 429])
        with self.assertRaises(ValueError):
            parse_retry_status_codes("99")

    def test_storage_wrapper_optional_modify_fields_are_envelope_metadata(self) -> None:
        source = notebook_source(self.nb, "code")
        for expected in [
            'create_user_col = "__acz_payload_create_user"',
            'create_time_col = "__acz_payload_create_time"',
            "F.col(create_user_col).isNotNull()",
            "F.col(create_time_col).isNotNull()",
            '("createUser", create_user_col)',
            '("createTime", create_time_col)',
        ]:
            self.assertIn(expected, source)

        envelope_fields = top_level_assignment_value(self.nb, "_ENVELOPE_FIELD_NAMES")
        for field_name in ["modifyUser", "modifyTime", "modify_user", "modify_time"]:
            self.assertIn(field_name, envelope_fields)

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

    def test_merge_key_parser_and_condition(self) -> None:
        parse_merge_key_columns = extract_function(self.nb, "_parse_merge_key_columns")
        self.assertEqual(parse_merge_key_columns(["id", "version"]), ["id", "version"])
        self.assertEqual(parse_merge_key_columns("id,version"), ["id", "version"])
        self.assertEqual(parse_merge_key_columns('["id", "version"]'), ["id", "version"])
        self.assertEqual(parse_merge_key_columns(""), ["id", "version"])
        with self.assertRaises(ValueError):
            parse_merge_key_columns([])

        merge_condition = extract_function(self.nb, "_merge_condition")
        self.assertEqual(
            merge_condition("target", "source", ["id", "version"]),
            "target.`id` = source.`id` AND target.`version` = source.`version`",
        )

    def test_incremental_watermark_limit_guard(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "_watermark_active",
                "validate_incremental_limit_safety",
            ],
        )
        validate_incremental_limit_safety = funcs["validate_incremental_limit_safety"]

        validate_incremental_limit_safety(False, "modifyTime", "auto", 10, {"WellLog": 5})
        validate_incremental_limit_safety(True, "", "auto", 10, {"WellLog": 5})
        validate_incremental_limit_safety(True, "modifyTime", "off", 10, {"WellLog": 5})
        validate_incremental_limit_safety(True, "modifyTime", "auto", None, {})

        with self.assertRaisesRegex(ValueError, "Watermark-based upsert cannot run with LIMIT"):
            validate_incremental_limit_safety(True, "modifyTime", "auto", 10, {})
        with self.assertRaisesRegex(ValueError, "Watermark-based upsert cannot run with LIMIT"):
            validate_incremental_limit_safety(True, "modifyTime", "required", None, {"WellLog": 5})

    def test_wildcard_kind_selectors(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "is_all_kinds_selector",
                "is_kind_pattern",
                "kind_pattern_to_regex",
                "matches_kind_selector",
                "_clean_kind_selectors",
                "_clean_optional_kind_selectors",
                "_kind_matches_selectors",
                "_filter_excluded_kinds",
                "kind_selectors_require_discovery",
                "resolve_kind_selectors",
                "resolve_affected_kind_selectors",
            ],
        )
        is_all_kinds_selector = funcs["is_all_kinds_selector"]
        is_kind_pattern = funcs["is_kind_pattern"]
        kind_pattern_to_regex = funcs["kind_pattern_to_regex"]
        matches_kind_selector = funcs["matches_kind_selector"]
        clean_kind_selectors = funcs["_clean_kind_selectors"]
        clean_optional_kind_selectors = funcs["_clean_optional_kind_selectors"]
        filter_excluded_kinds = funcs["_filter_excluded_kinds"]
        kind_selectors_require_discovery = funcs["kind_selectors_require_discovery"]
        resolve_kind_selectors = funcs["resolve_kind_selectors"]
        resolve_affected_kind_selectors = funcs["resolve_affected_kind_selectors"]

        welllog = "osdu:wks:work-product-component--WellLog:1.4.0"
        wellbore = "osdu:wks:master-data--Wellbore:1.2.0"
        reference = "osdu:wks:reference-data--UnitOfMeasure:1.0.0"

        self.assertTrue(is_all_kinds_selector("*:*:*:*"))
        self.assertTrue(is_all_kinds_selector("*"))
        self.assertTrue(is_all_kinds_selector("ALL"))
        self.assertTrue(is_kind_pattern("osdu:wks:*:*"))
        self.assertTrue(kind_pattern_to_regex("*:wks:work-product-component--Well*:1.*").match(welllog))
        self.assertTrue(matches_kind_selector(welllog, "*:wks:work-product-component--Well*:1.*"))
        self.assertFalse(matches_kind_selector(wellbore, "*:wks:work-product-component--Well*:1.*"))
        self.assertEqual(clean_kind_selectors([" ", welllog, welllog, "all"]), [welllog, "all"])
        self.assertEqual(clean_optional_kind_selectors([" ", reference, reference]), [reference])
        self.assertEqual(filter_excluded_kinds([welllog, reference], ["osdu:wks:reference*:*"]), [welllog])
        self.assertTrue(kind_selectors_require_discovery([welllog, "osdu:wks:*:*"]))
        self.assertFalse(kind_selectors_require_discovery([welllog]))

        resolve_kind_selectors.__globals__["discover_bronze_kinds"] = lambda *args, **kwargs: [welllog, wellbore, reference]
        self.assertEqual(
            resolve_kind_selectors(["*:*:*:*"], None, "", "", "osducatalog", excluded_selectors=["osdu:wks:reference*:*"]),
            [welllog, wellbore],
        )
        self.assertEqual(
            resolve_kind_selectors([welllog, reference], None, "", "", "osducatalog", excluded_selectors=["osdu:wks:reference*:*"]),
            [welllog],
        )

        resolve_affected_kind_selectors.__globals__["discover_bronze_kinds"] = lambda *args, **kwargs: [welllog]
        self.assertEqual(
            resolve_affected_kind_selectors([welllog, wellbore], None, "", "", "osducatalog", object()),
            [welllog],
        )
        self.assertEqual(
            resolve_affected_kind_selectors(["*:wks:work-product-component--Well*:1.*"], None, "", "", "osducatalog", object()),
            [welllog],
        )
        resolve_affected_kind_selectors.__globals__["discover_bronze_kinds"] = lambda *args, **kwargs: [welllog, reference]
        self.assertEqual(
            resolve_affected_kind_selectors(["all"], None, "", "", "osducatalog", object(), excluded_selectors=["osdu:wks:reference*:*"]),
            [welllog],
        )
        resolve_affected_kind_selectors.__globals__["discover_bronze_kinds"] = lambda *args, **kwargs: []
        self.assertEqual(resolve_affected_kind_selectors(["all"], None, "", "", "osducatalog", object()), [])

    def test_kind_to_table_name(self) -> None:
        kind_to_table_name = extract_function(self.nb, "kind_to_table_name")
        self.assertEqual(
            kind_to_table_name("osdu:wks:work-product-component--WellLog:1.4.0"),
            "osdu_wks_welllog",
        )
        self.assertEqual(
            kind_to_table_name("osdu:wks:master-data--Well:1.2.0"),
            "osdu_wks_well",
        )
        self.assertEqual(kind_to_table_name("Custom-Entity.Name"), "custom_entity_name")

    def test_child_table_names_use_full_normalized_paths(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "_sanitize_table_name_part",
                "_child_table_suffix",
                "_child_table_prefix",
                "child_table_name",
            ],
        )
        child_table_name = funcs["child_table_name"]
        suffix = funcs["_child_table_suffix"]

        self.assertEqual(suffix("data__curves"), "curves")
        self.assertEqual(suffix("data__LogData__Curves"), "logdata__curves")
        self.assertEqual(child_table_name("welllog", "data__LogData__Curves"), "welllog___logdata__curves")
        self.assertNotEqual(
            child_table_name("welllog", "data__A__items"),
            child_table_name("welllog", "data__B__items"),
        )

    def test_nested_property_assignment_for_inferred_schema(self) -> None:
        assign_nested_property = extract_function(self.nb, "_assign_nested_property")
        properties: dict[str, Any] = {}

        assign_nested_property(properties, ["LogData", "Curves", "Mnemonic"], {"type": "string"})

        self.assertEqual(
            properties,
            {
                "LogData": {
                    "type": "object",
                    "properties": {
                        "Curves": {
                            "type": "object",
                            "properties": {
                                "Mnemonic": {"type": "string"},
                            },
                        },
                    },
                },
            },
        )

    def test_json_schema_compatibility_helpers(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "_schema_definitions",
                "_definition_key_from_ref",
                "_first_non_null_json_type",
            ],
        )

        self.assertEqual(
            funcs["_schema_definitions"](
                {
                    "definitions": {"legacy": {"type": "string"}},
                    "$defs": {"modern": {"type": "integer"}},
                }
            ),
            {
                "legacy": {"type": "string"},
                "modern": {"type": "integer"},
            },
        )
        self.assertEqual(funcs["_definition_key_from_ref"]("#/definitions/legacy"), "legacy")
        self.assertEqual(funcs["_definition_key_from_ref"]("#/$defs/modern"), "modern")
        self.assertIsNone(funcs["_definition_key_from_ref"]("https://example.invalid/schema.json"))
        self.assertEqual(funcs["_first_non_null_json_type"](["null", "string"]), "string")
        self.assertEqual(funcs["_first_non_null_json_type"](["integer", "null"]), "integer")

    @unittest.skipIf(SparkTypes is None, "PySpark is not installed")
    def test_case_insensitive_struct_fields_are_deduplicated(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "_with_struct_field_type",
                "_merge_struct_fields",
                "_dedupe_case_insensitive_struct_type",
                "_merge_struct_types",
                "_merge_schemas",
            ],
        )
        T = SparkTypes
        registry_schema = T.StructType(
            [
                T.StructField("Description", T.StringType(), True),
                T.StructField(
                    "Nested",
                    T.StructType([T.StructField("Name", T.StringType(), True)]),
                    True,
                ),
            ]
        )
        inferred_schema = T.StructType(
            [
                T.StructField("description", T.StringType(), True),
                T.StructField(
                    "Nested",
                    T.StructType(
                        [
                            T.StructField("name", T.StringType(), True),
                            T.StructField("Value", T.IntegerType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

        merged = funcs["_merge_schemas"](registry_schema, inferred_schema)

        self.assertEqual([field.name for field in merged.fields], ["Description", "Nested"])
        nested = merged["Nested"].dataType
        self.assertEqual([field.name for field in nested.fields], ["Name", "Value"])

        array_schema = T.ArrayType(
            T.StructType(
                [
                    T.StructField("Description", T.StringType(), True),
                    T.StructField("description", T.IntegerType(), True),
                ]
            )
        )
        deduped_array = funcs["_dedupe_case_insensitive_struct_type"](array_schema)
        self.assertEqual([field.name for field in deduped_array.elementType.fields], ["Description"])

    def test_data_quality_configuration_helpers(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "_data_quality_enabled",
                "_parameter_int",
                "_data_quality_max_examples",
                "_data_quality_issues_table_name",
            ],
        )

        self.assertTrue(funcs["_data_quality_enabled"]())
        self.assertEqual(funcs["_data_quality_max_examples"](), 100)
        self.assertEqual(funcs["_data_quality_issues_table_name"](), "silver_data_quality_issues")

        funcs["_data_quality_enabled"].__globals__["data_quality_checks"] = False
        funcs["_data_quality_max_examples"].__globals__["data_quality_max_examples"] = 0
        funcs["_data_quality_issues_table_name"].__globals__["data_quality_issues_table"] = "custom_quality"

        self.assertFalse(funcs["_data_quality_enabled"]())
        self.assertEqual(funcs["_data_quality_max_examples"](), 0)
        self.assertEqual(funcs["_data_quality_issues_table_name"](), "custom_quality")

    def test_performance_configuration_helpers(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "_schema_fetch_parallelism",
                "_output_write_parallelism_for_sku",
                "_wide_max_cardinality_cap",
            ],
        )

        self.assertEqual(funcs["_schema_fetch_parallelism"](), 4)
        self.assertEqual(funcs["_output_write_parallelism_for_sku"]("F64"), 4)
        self.assertEqual(funcs["_output_write_parallelism_for_sku"]("F128"), 6)
        self.assertEqual(funcs["_output_write_parallelism_for_sku"]("F256"), 8)
        self.assertEqual(funcs["_output_write_parallelism_for_sku"]("P1"), 4)
        self.assertEqual(funcs["_output_write_parallelism_for_sku"]("FTL4"), 1)
        self.assertEqual(funcs["_wide_max_cardinality_cap"](), 20)

        funcs["_schema_fetch_parallelism"].__globals__["schema_fetch_parallelism"] = 8
        funcs["_wide_max_cardinality_cap"].__globals__["wide_max_cardinality_cap"] = 5

        self.assertEqual(funcs["_schema_fetch_parallelism"](), 8)
        self.assertEqual(funcs["_wide_max_cardinality_cap"](), 5)

    def test_output_tables_from_results_deduplicates_tables(self) -> None:
        output_tables_from_results = extract_function(self.nb, "_output_tables_from_results")

        class Result:
            def __init__(self, parent_table: str, child_tables: list[str] | None = None) -> None:
                self.parent_table = parent_table
                self.child_tables = child_tables

        self.assertEqual(
            output_tables_from_results(
                [
                    Result("welllog", ["welllog___curves", "welllog___curves"]),
                    Result("welllog", ["welllog___parameters"]),
                ],
                ["fallback"],
            ),
            ["welllog", "welllog___curves", "welllog___parameters"],
        )
        self.assertEqual(output_tables_from_results([], ["planned_parent"]), ["planned_parent"])

    def test_version_strategy_helpers(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "kind_parts",
                "kind_family_key",
                "kind_version",
                "kind_to_table_name",
                "kind_to_versioned_table_name",
                "group_kinds_by_version_strategy",
                "table_name_for_kind_group",
                "detect_table_collisions",
            ],
        )
        family = funcs["kind_family_key"]
        version = funcs["kind_version"]
        versioned = funcs["kind_to_versioned_table_name"]
        group = funcs["group_kinds_by_version_strategy"]
        collisions = funcs["detect_table_collisions"]

        kinds = [
            "osdu:wks:master-data--Organisation:1.0.0",
            "osdu:wks:master-data--Organisation:1.2.0",
        ]
        self.assertEqual(family(kinds[0]), "osdu:wks:master-data--Organisation")
        self.assertEqual(version(kinds[1]), "1.2.0")
        self.assertEqual(versioned(kinds[1]), "osdu_wks_organisation__v1_2_0")
        self.assertEqual(
            versioned("data:wks:dataset--File.Generic:1.0.0"),
            "data_wks_file_generic__v1_0_0",
        )
        self.assertFalse(
            collisions(
                [
                    "data:wks:dataset--File.Generic:1.0.0",
                    "osdu:wks:dataset--File.Generic:1.0.0",
                ],
                "",
                "versioned_tables",
            )
        )
        self.assertEqual(len(group(kinds, "merge")), 1)
        self.assertEqual(len(group(kinds, "versioned_tables")), 2)
        self.assertTrue(collisions(kinds, "", "merge")[0]["safe"])

    def test_adme_schema_url_uses_configured_endpoint(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "_adme_schema_config",
                "_adme_auth_method",
                "_adme_managed_identity_client_id",
                "_adme_keyvault_url",
                "_schema_doc_cache_key",
                "_adme_schema_url",
                "_adme_schema_list_url",
                "_adme_schema_source_prefix",
                "_adme_schema_source",
            ],
        )
        self.assertEqual(
            funcs["_adme_schema_config"](),
            (
                "https://contoso.energy.azure.com",
                "data",
                "https://management.core.windows.net/.default",
            ),
        )
        self.assertEqual(funcs["_adme_auth_method"](), "SP")
        funcs["_adme_auth_method"].__globals__["adme_auth_method"] = "MI"
        funcs["_adme_auth_method"].__globals__["adme_managed_identity_client_id"] = "33333333-3333-3333-3333-333333333333"
        self.assertEqual(funcs["_adme_auth_method"](), "MI")
        self.assertEqual(funcs["_adme_managed_identity_client_id"](), "33333333-3333-3333-3333-333333333333")
        funcs["_adme_auth_method"].__globals__["adme_auth_method"] = "SP"
        self.assertEqual(funcs["_adme_keyvault_url"]("contoso-kv"), "https://contoso-kv.vault.azure.net/")
        self.assertEqual(
            funcs["_adme_keyvault_url"]("https://contoso-kv.vault.azure.net/"),
            "https://contoso-kv.vault.azure.net/",
        )
        self.assertEqual(
            funcs["_adme_schema_url"]("osdu:wks:reference-data--DurationContext:1.0.0"),
            "https://contoso.energy.azure.com/api/schema-service/v1/schema/osdu:wks:reference-data--DurationContext:1.0.0",
        )
        self.assertEqual(
            funcs["_adme_schema_list_url"](),
            "https://contoso.energy.azure.com/api/schema-service/v1/schema?latestVersion=False&limit=1",
        )
        self.assertEqual(
            funcs["_adme_schema_list_url"](0),
            "https://contoso.energy.azure.com/api/schema-service/v1/schema?latestVersion=False&limit=1",
        )
        self.assertEqual(
            funcs["_schema_doc_cache_key"]("osdu:wks:reference-data--DurationContext:1.0.0"),
            (
                "https://contoso.energy.azure.com",
                "data",
                "osdu:wks:reference-data--DurationContext:1.0.0",
            ),
        )
        self.assertEqual(
            funcs["_adme_schema_source_prefix"](),
            "adme:endpoint=https://contoso.energy.azure.com;partition=data;",
        )
        self.assertEqual(
            funcs["_adme_schema_source"](
                "https://contoso.energy.azure.com/api/schema-service/v1/schema/osdu:wks:reference-data--DurationContext:1.0.0"
            ),
            "adme:endpoint=https://contoso.energy.azure.com;partition=data;url=https://contoso.energy.azure.com/api/schema-service/v1/schema/osdu:wks:reference-data--DurationContext:1.0.0",
        )

        funcs["_adme_schema_config"].__globals__["ADME_TOKEN_SCOPE"] = ""
        with self.assertRaises(ValueError):
            funcs["_adme_schema_config"]()
        funcs["_adme_auth_method"].__globals__["adme_auth_method"] = "DC"
        self.assertEqual(funcs["_adme_auth_method"](), "DC")
        funcs["_adme_auth_method"].__globals__["adme_auth_method"] = "invalid"
        with self.assertRaises(ValueError):
            funcs["_adme_auth_method"]()

    def test_managed_identity_token_path_does_not_require_tenant(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "_adme_schema_config",
                "_adme_auth_method",
                "_adme_auth_value",
                "_adme_authority_url",
                "_adme_managed_identity_client_id",
                "_adme_managed_identity_credential",
                "_acquire_adme_access_token",
            ],
        )

        class FakeAccessToken:
            token = "token-value"
            expires_on = 1234567890

        class FakeManagedIdentityCredential:
            def __init__(self, **kwargs) -> None:
                self.kwargs = kwargs

            def get_token(self, scope: str):
                self.scope = scope
                return FakeAccessToken()

        globals_ = funcs["_acquire_adme_access_token"].__globals__
        globals_["ManagedIdentityCredential"] = FakeManagedIdentityCredential
        globals_["DefaultAzureCredential"] = None
        globals_["adme_auth_method"] = "MI"
        globals_["adme_tenant_id"] = ""
        globals_["adme_managed_identity_client_id"] = "33333333-3333-3333-3333-333333333333"

        self.assertEqual(funcs["_acquire_adme_access_token"](), ("token-value", 1234567890))

    def test_adme_schema_response_unwrapping(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "_looks_like_json_schema",
                "_coerce_schema_body",
                "_extract_adme_schema_doc",
            ],
        )
        schema_doc = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "data": {
                    "type": "object",
                    "properties": {"Name": {"type": "string"}},
                }
            },
        }
        extract = funcs["_extract_adme_schema_doc"]

        self.assertTrue(funcs["_looks_like_json_schema"](schema_doc))
        self.assertIs(extract(schema_doc, "osdu:wks:reference-data--DurationContext:1.0.0"), schema_doc)
        self.assertEqual(
            extract(
                {
                    "schemaInfo": {"schemaIdentity": {"id": "osdu:wks:reference-data--DurationContext:1.0.0"}},
                    "schema": schema_doc,
                },
                "osdu:wks:reference-data--DurationContext:1.0.0",
            ),
            schema_doc,
        )
        self.assertEqual(
            extract(
                {"schema": json.dumps(schema_doc)},
                "osdu:wks:reference-data--DurationContext:1.0.0",
            ),
            schema_doc,
        )
        with self.assertRaises(ValueError):
            extract({"schemaInfo": {"schemaIdentity": {"id": "bad"}}}, "bad")


if __name__ == "__main__":
    unittest.main()
