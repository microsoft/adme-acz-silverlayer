"""Parameter-cell execution tests without Fabric, Spark, or Azure credentials."""

import ast
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from adme_acz_silverlayer.config import NOTEBOOK_PARAMETER_TYPES, resolve_notebook_parameters
from adme_acz_silverlayer.notebook_sync import clean_notebook, validation_issues


class NotebookParameterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.notebook = json.loads((ROOT / "ADME ACZ Silver Layer.ipynb").read_text())
        cls.parameter_index = next(
            i for i, cell in enumerate(cls.notebook["cells"])
            if "parameters" in cell.get("metadata", {}).get("tags", [])
        )
        cls.defaults_source = "".join(cls.notebook["cells"][cls.parameter_index]["source"])
        cls.resolver_source = "".join(cls.notebook["cells"][cls.parameter_index + 1]["source"])
        namespace = {}
        exec(cls.defaults_source, namespace)
        cls.defaults = {name: namespace[name] for name in NOTEBOOK_PARAMETER_TYPES}

    def execute_configuration(self, overrides=None, environment=None, interactive=None):
        namespace = {}
        # Jupyter executes edited defaults; Fabric then injects assignments.
        exec(self.defaults_source, namespace)
        namespace.update(interactive or {})
        injected = "\n".join(f"{key} = {value!r}" for key, value in (overrides or {}).items())
        exec(injected, namespace)
        with patch.dict(os.environ, environment or {}, clear=True), contextlib.redirect_stdout(io.StringIO()):
            exec(self.resolver_source, namespace)
        return namespace

    def connected_settings(self):
        return {
            "WORKSPACE_ID": "test-workspace", "LAKEHOUSE_ID": "test-lakehouse",
            "ADME_ENDPOINT": "https://example.energy.azure.com", "ADME_DATA_PARTITION_ID": "test",
            "ADME_AUTH_METHOD": "MI", "OUTPUT_WRITE_PARALLELISM": 1,
        }

    def test_exactly_one_assignment_only_parameter_cell_and_complete_contract(self):
        cells = [c for c in self.notebook["cells"] if "parameters" in c.get("metadata", {}).get("tags", [])]
        self.assertEqual(len(cells), 1)
        nodes = ast.parse(self.defaults_source).body
        self.assertTrue(all(isinstance(node, ast.Assign) for node in nodes))
        names = {node.targets[0].id for node in nodes}
        self.assertEqual(names, set(NOTEBOOK_PARAMETER_TYPES))
        self.assertNotIn("NOTEBOOK_VERSION", names)
        self.assertFalse(any("injected-parameters" in c.get("metadata", {}).get("tags", []) for c in self.notebook["cells"]))
        self.assertEqual(clean_notebook(self.notebook), self.notebook)
        self.assertEqual(validation_issues(self.notebook), [])

    def test_defaults_are_preserved(self):
        self.assertEqual(resolve_notebook_parameters(self.defaults, {}), self.defaults)

    def test_injected_false_zero_empty_values_reach_effective_snapshot(self):
        values = {
            **self.connected_settings(), "KINDS": '["osdu:wks:master-data--Well:*"]', "EXCLUDED_KINDS": "",
            "KIND_LIMITS": "{}", "LIMIT": 0, "ALLOW_OVERWRITE": False,
            "WRITE_RELATIONSHIP_BRIDGES": "false", "TABLE_PREFIX": "",
            "ADME_SCHEMA_RETRY_TOTAL": 0, "ADME_SCHEMA_RETRY_BACKOFF_SECONDS": 0,
            "ADME_SCHEMA_RETRY_STATUS_CODES": "[]", "INCREMENTAL_WATERMARK_COLUMN": "",
        }
        ns = self.execute_configuration(values, interactive={
            "LIMIT": 10, "TABLE_PREFIX": "preview_", "ALLOW_OVERWRITE": True,
            "KINDS": ["example"], "KIND_LIMITS": {"example": 10},
        })
        snapshot = ns["config_snapshot"]
        self.assertEqual(snapshot["kind_selectors"], ["osdu:wks:master-data--Well:*"])
        self.assertEqual(snapshot["excluded_kind_selectors"], [])
        self.assertEqual(snapshot["kind_limits"], {})
        self.assertIsNone(snapshot["limit"])
        self.assertIs(snapshot["allow_overwrite"], False)
        self.assertIs(snapshot["write_relationship_bridges"], False)
        self.assertEqual(snapshot["table_prefix"], "")
        self.assertEqual(snapshot["adme_schema_retry_total"], 0)
        self.assertEqual(snapshot["adme_schema_retry_backoff_seconds"], 0)
        self.assertEqual(snapshot["adme_schema_retry_status_codes"], [])
        self.assertEqual(snapshot["watermark_column"], "")

    def test_jupyter_defaults_and_injected_values_have_same_contract(self):
        values = {**self.connected_settings(), "KINDS": ["a", "b"], "KIND_LIMITS": {"a": 0}}
        interactive = self.execute_configuration(interactive=values)
        injected = self.execute_configuration(overrides=values)
        self.assertEqual(interactive["config_snapshot"], injected["config_snapshot"])
        self.assertEqual(interactive["config_hash"], injected["config_hash"])

    def test_all_boolean_parameters_reach_effective_snapshot(self):
        for name, kind in NOTEBOOK_PARAMETER_TYPES.items():
            if kind != "boolean":
                continue
            for value in (False, True):
                with self.subTest(name=name, value=value):
                    ns = self.execute_configuration({
                        **self.connected_settings(), "OUTPUT_DOCS_MODE": "full", name: value,
                    })
                    self.assertIs(ns["config_snapshot"][name.lower()], value)

    def test_json_and_native_collections_are_equivalent(self):
        values = {**self.connected_settings(), "KINDS": ["a", "b"], "EXCLUDED_KINDS": ["c"],
                  "KIND_LIMITS": {"a": 0}, "MERGE_KEY_COLUMNS": ["id"],
                  "ADME_SCHEMA_RETRY_STATUS_CODES": [429, 503]}
        encoded = {name: json.dumps(value) if isinstance(value, (list, dict)) else value
                   for name, value in values.items()}
        native = self.execute_configuration(values)
        strings = self.execute_configuration(encoded)
        self.assertEqual(native["config_snapshot"], strings["config_snapshot"])

    def test_environment_wins_by_presence_including_empty_values(self):
        ns = self.execute_configuration(
            {**self.connected_settings(), "KINDS": ["a"], "TABLE_PREFIX": "test_",
             "LIMIT": 5, "WRITE_RELATIONSHIP_BRIDGES": True, "KIND_LIMITS": {"a": 5}},
            {"ADME_KINDS": "b", "ADME_TABLE_PREFIX": "", "ADME_LIMIT": "0",
             "ADME_WRITE_RELATIONSHIP_BRIDGES": "false", "ADME_KIND_LIMITS": "{}"},
        )
        self.assertEqual(ns["kind_selectors"], ["b"])
        self.assertEqual(ns["table_prefix"], "")
        self.assertIsNone(ns["limit"])
        self.assertIs(ns["write_relationship_bridges"], False)
        self.assertEqual(ns["kind_limits"], {})

    def test_environment_overrides_do_not_persist_into_parameter_values(self):
        ns = self.execute_configuration(
            {**self.connected_settings(), "TABLE_PREFIX": "parameter_"},
            {"ADME_TABLE_PREFIX": "environment_"},
        )
        self.assertEqual(ns["table_prefix"], "environment_")
        self.assertEqual(ns["TABLE_PREFIX"], "parameter_")
        with patch.dict(os.environ, {}, clear=True), contextlib.redirect_stdout(io.StringIO()):
            exec(self.resolver_source, ns)
        self.assertEqual(ns["table_prefix"], "parameter_")

    def test_every_parameter_supports_its_environment_alias(self):
        examples = {
            "string": "example", "boolean": False, "nonnegative_integer": 0,
            "positive_integer": 2, "string_list": ["example"], "kind_limits": {"example": 0},
            "status_codes": [429], "nonnegative_number": 0.0, "parallelism": 2,
        }
        for name, kind in NOTEBOOK_PARAMETER_TYPES.items():
            with self.subTest(name=name):
                value = examples[kind]
                env_name = name if name.startswith("ADME_") else f"ADME_{name}"
                raw = value if isinstance(value, str) else json.dumps(value)
                self.assertEqual(resolve_notebook_parameters(self.defaults, {env_name: raw})[name], value)

    def test_environment_collection_formats_and_explicit_modes(self):
        ns = self.execute_configuration(
            {**self.connected_settings(), "WRITE_MODE": "full_refresh"},
            {"ADME_KINDS": "a,b", "ADME_KIND_LIMITS": "a=0;b=2",
             "ADME_MERGE_KEY_COLUMNS": "id,version", "ADME_SCHEMA_RETRY_STATUS_CODES": "429,503",
             "ADME_WRITE_MODE": "upsert", "ADME_OUTPUT_MODE": "wide"},
        )
        self.assertEqual(ns["kind_selectors"], ["a", "b"])
        self.assertEqual(ns["kind_limits"], {"a": 0, "b": 2})
        self.assertEqual(ns["write_mode"], "upsert")
        self.assertEqual(ns["output_mode"], "wide")
        ns = self.execute_configuration(self.connected_settings(), {
            "ADME_WRITE_MODE": "full_refresh", "ADME_OUTPUT_MODE": "normalized",
        })
        self.assertEqual(ns["write_mode"], "full_refresh")
        self.assertEqual(ns["output_mode"], "normalized")

    def test_retired_mode_aliases_fail_even_with_explicit_modes(self):
        for alias, replacement, mode in (
            ("ADME_INCREMENTAL", "ADME_WRITE_MODE", "full_refresh"),
            ("ADME_REASSEMBLE", "ADME_OUTPUT_MODE", "normalized"),
        ):
            for value in ("", "false", "true", "0", "no", "invalid"):
                for modern in ({}, {replacement: mode}):
                    environment = {alias: value, **modern}
                    with self.subTest(environment=environment):
                        with self.assertRaisesRegex(ValueError, f"{alias}.*{replacement}"):
                            self.execute_configuration(self.connected_settings(), environment)
                        with self.assertRaisesRegex(ValueError, f"{alias}.*{replacement}"):
                            resolve_notebook_parameters(self.defaults, environment)

    def test_blank_environment_cannot_erase_configured_destination_or_identity(self):
        for name in ("WORKSPACE_ID", "LAKEHOUSE_ID", "ADME_MANAGED_IDENTITY_CLIENT_ID"):
            env_name = name if name.startswith("ADME_") else f"ADME_{name}"
            settings = {**self.defaults, name: "configured-id"}
            for blank in ("", " \t "):
                environment = {env_name: blank}
                with self.subTest(name=name, blank=blank):
                    with self.assertRaisesRegex(ValueError, f"{env_name}.*unset"):
                        self.execute_configuration({**self.connected_settings(), name: "configured-id"}, environment)
                    with self.assertRaisesRegex(ValueError, f"{env_name}.*unset"):
                        resolve_notebook_parameters(settings, environment)
                    self.assertEqual(settings[name], "configured-id")
                    self.assertEqual(environment[env_name], blank)
                    blank_settings = {**self.defaults, name: ""}
                    self.assertEqual(resolve_notebook_parameters(blank_settings, environment)[name], blank)
            self.assertEqual(resolve_notebook_parameters(settings, {})[name], "configured-id")
            self.assertEqual(
                resolve_notebook_parameters(settings, {env_name: "selected-id"})[name], "selected-id",
            )
        ns = self.execute_configuration(
            {**self.connected_settings(), "ADME_MANAGED_IDENTITY_CLIENT_ID": ""},
            {"ADME_MANAGED_IDENTITY_CLIENT_ID": ""},
        )
        self.assertEqual(ns["adme_managed_identity_client_id"], "")

    def test_quality_example_cap_representations_reach_effective_snapshot(self):
        for value in (0, "0", 1, "1", 100, "100"):
            settings = {**self.connected_settings(), "DATA_QUALITY_MAX_EXAMPLES": value}
            for namespace in (
                self.execute_configuration(overrides=settings),
                self.execute_configuration(interactive=settings),
                self.execute_configuration(self.connected_settings(), {"ADME_DATA_QUALITY_MAX_EXAMPLES": str(value)}),
            ):
                with self.subTest(value=value):
                    self.assertEqual(namespace["config_snapshot"]["data_quality_max_examples"], int(value))
            self.assertEqual(
                resolve_notebook_parameters({**self.defaults, "DATA_QUALITY_MAX_EXAMPLES": value}, {})[
                    "DATA_QUALITY_MAX_EXAMPLES"
                ],
                int(value),
            )

    def test_invalid_overrides_fail_before_runtime_resolution(self):
        cases = {
            "ALLOW_OVERWRITE": ["invalid", "", None, 1],
            "LIMIT": [-1, True, 1.5, "", "1.5"],
            "DATA_QUALITY_MAX_EXAMPLES": [-1, "-1", True, False, 1.5, "1.5", "", "invalid"],
            "METADATA_FLUSH_INTERVAL": [0],
            "OUTPUT_WRITE_PARALLELISM": [0, False, "invalid"],
            "ADME_SCHEMA_RETRY_BACKOFF_SECONDS": [-1, "nan", "inf", True, 10**400],
            "KINDS": ['{"kind":"a"}', "[", [1], None, [""], [], "", "[]"],
            "KIND_LIMITS": ['[]', {"a": True}, {"a": -1}, {"": 2}],
            "MERGE_KEY_COLUMNS": [[], "", [False]],
            "ADME_SCHEMA_RETRY_STATUS_CODES": [[99], [600], [True], "[null]"],
            "BRONZE_TABLE": ["", None],
            "OUTPUT_MODE": [""],
        }
        for name, values in cases.items():
            for value in values:
                with self.subTest(name=name, value=value):
                    with self.assertRaisesRegex(ValueError, name):
                        self.execute_configuration({name: value})
                    if isinstance(value, str):
                        env = name if name.startswith("ADME_") else f"ADME_{name}"
                        with self.assertRaisesRegex(ValueError, name):
                            self.execute_configuration(environment={env: value})

    def test_invalid_resolution_does_not_mutate_input(self):
        settings = copy.deepcopy(self.defaults)
        with self.assertRaises(ValueError):
            resolve_notebook_parameters(settings, {"ADME_LIMIT": "-1"})
        self.assertEqual(settings, self.defaults)

    def test_invalid_modes_retain_existing_validation(self):
        for name in (
            "RUN_PROFILE", "WRITE_MODE", "OUTPUT_MODE", "VERSION_STRATEGY",
            "MISSING_SCHEMA_MODE", "OUTPUT_DOCS_MODE", "INCREMENTAL_WATERMARK_MODE",
            "ADME_AUTH_METHOD",
        ):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, name):
                self.execute_configuration({**self.connected_settings(), name: "invalid"})

    def test_pipeline_sample_uses_native_names_and_supported_values(self):
        sample = json.loads((ROOT / "samples/fabric_pipeline_parameters.json").read_text())
        self.assertLessEqual(sample["parameters"].keys(), NOTEBOOK_PARAMETER_TYPES.keys())
        resolve_notebook_parameters({**self.defaults, **sample["parameters"]}, {})

    def test_authentication_requirements_are_preserved(self):
        for method, expected in (("SP", "ADME_TENANT_ID"), ("DC", "ADME_TENANT_ID")):
            with self.subTest(method=method), self.assertRaisesRegex(ValueError, expected):
                self.execute_configuration({**self.connected_settings(), "ADME_AUTH_METHOD": method})
        self.execute_configuration({
            **self.connected_settings(), "ADME_AUTH_METHOD": "SP", "ADME_TENANT_ID": "test-tenant",
            "ADME_SP_CLIENT_ID": "test-client", "ADME_SP_SECRET_KV_NAME": "test-vault",
            "ADME_SP_SECRET_NAME": "test-secret",
        })

    def test_required_watermark_rejects_explicit_empty_column(self):
        with self.assertRaisesRegex(ValueError, "INCREMENTAL_WATERMARK_COLUMN"):
            self.execute_configuration({
                **self.connected_settings(), "INCREMENTAL_WATERMARK_MODE": "required",
                "INCREMENTAL_WATERMARK_COLUMN": "",
            })


if __name__ == "__main__":
    unittest.main()
