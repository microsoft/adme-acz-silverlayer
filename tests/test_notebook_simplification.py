import ast
import hashlib
import json
import os
import re
import unittest
from pathlib import Path
from typing import Any
from urllib.parse import quote


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
                namespace: dict[str, object] = {
                    "hashlib": hashlib,
                    "json": json,
                    "os": os,
                    "Any": Any,
                    "KindResult": object,
                    "MERGE_KEY_COLUMNS": ["id", "version"],
                    "_DELTA_COLUMN_PART_RE": re.compile(r"[^0-9A-Za-z_]+"),
                }
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
        "MERGE_KEY_COLUMNS": ["id", "version"],
        "_DELTA_COLUMN_PART_RE": re.compile(r"[^0-9A-Za-z_]+"),
    }
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
            'os.environ.get("ADME_ENDPOINT")',
            'os.environ.get("ADME_DATA_PARTITION_ID")',
            'os.environ.get("ADME_AUTH_METHOD")',
            'os.environ.get("ADME_MANAGED_IDENTITY_CLIENT_ID")',
            'os.environ.get("ADME_TENANT_ID")',
            'os.environ.get("ADME_SP_CLIENT_ID")',
            'os.environ.get("ADME_SP_SECRET_KV_NAME")',
            'os.environ.get("ADME_SP_SECRET_NAME")',
            'NOTEBOOK_VERSION = "0.5.0"',
            "ALLOW_OVERWRITE = False",
            'MERGE_KEY_COLUMNS = ["id", "version"]',
            "ADME_MERGE_KEY_COLUMNS",
            "ADME_ALLOW_OVERWRITE",
            'WRITE_MODE = "full_refresh"',
            "ADME_WRITE_MODE",
            "ADME_INCREMENTAL",
            'INCREMENTAL_WATERMARK_COLUMN = ""',
            'INCREMENTAL_WATERMARK_MODE = "auto"',
            'INCREMENTAL_STATE_TABLE = "silver_incremental_state"',
            "ADME_INCREMENTAL_WATERMARK_COLUMN",
            "ADME_INCREMENTAL_WATERMARK_MODE",
            "ADME_INCREMENTAL_STATE_TABLE",
            "config_hash = hashlib.sha256",
            "PERSIST_SCHEMA_CACHE = True",
            'SCHEMA_CACHE_TABLE = "silver_schema_cache"',
            'RUN_MANIFEST_TABLE = "silver_run_manifest"',
            'RUN_STATUS_TABLE = "silver_run_status"',
            'schema_cache_writes_enabled = persist_schema_cache and run_profile == "full"',
            "ADME_PERSIST_SCHEMA_CACHE",
            "ADME_SCHEMA_CACHE_TABLE",
            "ADME_RUN_MANIFEST_TABLE",
            "ADME_RUN_STATUS_TABLE",
            "KIND_LIMITS = {}",
            "ADME_KIND_LIMITS",
            "kind_selectors",
            "WRITE_OUTPUT_DOCS = True",
            'OUTPUT_DOCS_TABLE = "silver_output_documentation"',
            "ADME_WRITE_OUTPUT_DOCS",
            "ADME_OUTPUT_DOCS_TABLE",
            "DATA_QUALITY_CHECKS = True",
            'DATA_QUALITY_ISSUES_TABLE = "silver_data_quality_issues"',
            "DATA_QUALITY_MAX_EXAMPLES = 100",
            "ADME_DATA_QUALITY_CHECKS",
            "ADME_DATA_QUALITY_ISSUES_TABLE",
            "ADME_DATA_QUALITY_MAX_EXAMPLES",
            'VERSION_STRATEGY = "versioned_tables"',
            'MISSING_SCHEMA_MODE = "skip"',
            "CREATE_EMPTY_CHILD_TABLES = True",
            "ADME_VERSION_STRATEGY",
            "ADME_MISSING_SCHEMA_MODE",
            "ADME_CREATE_EMPTY_CHILD_TABLES",
            "CACHE_BRONZE = True",
            'BRONZE_CACHE_STORAGE_LEVEL = "MEMORY_AND_DISK"',
            "PREFLIGHT_KIND_COUNTS = True",
            "BATCH_METADATA_WRITES = True",
            "METADATA_FLUSH_INTERVAL = 100",
            'OUTPUT_DOCS_MODE = "summary"',
            "SCHEMA_PREFLIGHT = True",
            "SCHEMA_FETCH_PARALLELISM = 4",
            "WIDE_MAX_CARDINALITY_CAP = 20",
            "ADME_CACHE_BRONZE",
            "ADME_PREFLIGHT_KIND_COUNTS",
            "ADME_BATCH_METADATA_WRITES",
            "ADME_OUTPUT_DOCS_MODE",
            "ADME_SCHEMA_PREFLIGHT",
            "ADME_SCHEMA_FETCH_PARALLELISM",
            "ADME_WIDE_MAX_CARDINALITY_CAP",
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
            "ADME_INCLUDE_INACTIVE_RECORDS",
            'include_inactive_records = _env_bool("ADME_INCLUDE_INACTIVE_RECORDS", INCLUDE_INACTIVE_RECORDS)',
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
            "def _normalize_bronze_record_wrapper(",
            'F.get_json_object(F.col("data"), "$.data")',
            "df = _normalize_bronze_record_wrapper(df)",
            "def prepare_bronze_df(",
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
            "def apply_incremental_watermark_filter(",
            "def write_incremental_watermark_state(",
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
            "def infer_schema_doc_from_bronze_df(",
            "def build_inferred_registry_from_bronze(",
            "def _fetch_schema_docs_from_adme(",
            "def _schema_fetch_parallelism(",
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
            "def write_run_manifest(",
            "def write_run_status(",
            "def _output_tables_from_results(",
            "def collect_data_quality_issues(",
            "def evaluate_and_write_data_quality_issues(",
            "def write_data_quality_issues(",
            "def write_output_documentation(",
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
            "_load_schema_doc_from_persistent_cache",
            "_write_schema_doc_to_persistent_cache",
            "silver_output_documentation",
            "silver_data_quality_issues",
            "Timing summary",
            "metadata_flush",
            "output_docs_flush",
            "silver_run_status",
            'final_status = "failed" if failed else "committed"',
            "outputs are not marked committed",
            "ADME schema retries",
        ]:
            self.assertIn(expected, source)

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
        self.assertIn("effective_cardinality_cap = max_cardinality_cap or _wide_max_cardinality_cap()", source)
        self.assertIn("_adme_schema_get_json(list_url", source)
        self.assertIn("_adme_schema_get_json(schema_url", source)
        self.assertIn("def validate_incremental_limit_safety(", source)
        self.assertIn(">= F.lit(previous).cast(data_type)", source)
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

    def test_data_quality_configuration_helpers(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "_data_quality_enabled",
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
        self.assertEqual(funcs["_data_quality_max_examples"](), 1)
        self.assertEqual(funcs["_data_quality_issues_table_name"](), "custom_quality")

    def test_performance_configuration_helpers(self) -> None:
        funcs = extract_functions(
            self.nb,
            [
                "_schema_fetch_parallelism",
                "_wide_max_cardinality_cap",
            ],
        )

        self.assertEqual(funcs["_schema_fetch_parallelism"](), 4)
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
