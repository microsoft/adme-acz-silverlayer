import ast
import json
import os
import sys
import unittest
from pathlib import Path
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
NOTEBOOK = ROOT / "ADME ACZ Silver Layer.ipynb"

sys.path.insert(0, str(SRC))

from adme_acz_silverlayer import adme_schema, bronze, fabric_io  # noqa: E402


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
        "os": os,
        "quote": quote,
        "ADME_SCHEMA_SERVICE_PATH": "/api/schema-service/v1/schema",
        "FABRIC_STORAGE_SCOPE": "https://storage.azure.com/.default",
        "DataFrame": object,
        "_adme_schema_config": lambda: (
            "https://contoso.energy.azure.com",
            "data",
            "https://management.core.windows.net/.default",
        ),
        "_onelake_table_uri": lambda workspace_id, lakehouse_id, table_name: fabric_io.onelake_table_uri(workspace_id, lakehouse_id, table_name),
        "table_uri": lambda workspace_id, lakehouse_id, table_name: fabric_io.table_uri(workspace_id, lakehouse_id, table_name),
    }
    exec(compile(module, filename="<runtime-boundary-functions>", mode="exec"), namespace)
    return {name: namespace[name] for name in function_names}


class RuntimeBoundaryModuleTests(unittest.TestCase):
    def test_fabric_io_helpers_match_notebook(self) -> None:
        funcs = extract_functions(
            [
                "_requires_path_fallback",
                "_table_path_uri",
                "table_uri",
                "_onelake_table_uri",
                "_fabric_storage_options",
                "_metadata_table_name",
                "_metadata_table_names",
            ]
        )
        funcs["_table_path_uri"].__globals__["workspace_id"] = "workspace"
        funcs["_table_path_uri"].__globals__["lakehouse_id"] = "lakehouse"

        for message in [
            "No default context found",
            "partial namespaces are unsupported",
            "Please attach a lakehouse",
            "other error",
        ]:
            self.assertEqual(fabric_io.requires_path_fallback(Exception(message)), funcs["_requires_path_fallback"](Exception(message)))

        self.assertEqual(fabric_io.onelake_table_uri("workspace", "lakehouse", "table"), funcs["_onelake_table_uri"]("workspace", "lakehouse", "table"))
        self.assertEqual(fabric_io.table_path_uri("table", "workspace", "lakehouse"), funcs["_table_path_uri"]("table"))
        self.assertEqual(fabric_io.table_uri("workspace", "lakehouse", "table"), funcs["table_uri"]("workspace", "lakehouse", "table"))
        self.assertEqual(fabric_io.metadata_table_name("workspace", "lakehouse", "table"), funcs["_metadata_table_name"]("workspace", "lakehouse", "table"))
        self.assertEqual(fabric_io.metadata_table_names("workspace", "lakehouse"), funcs["_metadata_table_names"]("workspace", "lakehouse"))

        class Credential:
            def get_token(self, scope: str):
                self.scope = scope

                class Token:
                    token = "token-value"

                return Token()

        credential = Credential()
        funcs["_fabric_storage_options"].__globals__["_FABRIC_CREDENTIAL"] = credential
        self.assertEqual(
            fabric_io.fabric_storage_options(credential, "https://storage.azure.com/.default"),
            funcs["_fabric_storage_options"](),
        )

    def test_bronze_filter_status_matches_notebook(self) -> None:
        funcs = extract_functions(["_include_inactive_records", "active_record_filter_status"])

        class Frame:
            def __init__(self, columns: list[str]) -> None:
                self.columns = columns

        self.assertEqual(bronze.include_inactive_records(), funcs["_include_inactive_records"]())
        self.assertEqual(
            bronze.active_record_filter_status(["id", "isActive"], False),
            funcs["active_record_filter_status"](Frame(["id", "isActive"]), False),
        )
        self.assertEqual(
            bronze.active_record_filter_status(["id"], False),
            funcs["active_record_filter_status"](Frame(["id"]), False),
        )
        self.assertEqual(
            bronze.active_record_filter_status(["id"], True),
            funcs["active_record_filter_status"](Frame(["id"]), True),
        )

    def test_adme_schema_helpers_match_notebook(self) -> None:
        funcs = extract_functions(
            [
                "_adme_auth_method",
                "_adme_auth_value",
                "_adme_authority_url",
                "_adme_managed_identity_client_id",
                "_adme_keyvault_url",
                "_schema_doc_cache_key",
                "_adme_schema_url",
                "_adme_schema_source_prefix",
                "_adme_schema_source",
                "_adme_schema_list_url",
                "_adme_response_detail",
                "_adme_schema_timeout",
                "_adme_schema_retry_status_codes",
                "_adme_schema_retry_summary",
            ]
        )

        self.assertEqual(adme_schema.adme_auth_method("sp"), funcs["_adme_auth_method"]())
        funcs["_adme_auth_method"].__globals__["adme_auth_method"] = "MI"
        self.assertEqual(adme_schema.adme_auth_method("MI"), funcs["_adme_auth_method"]())
        with self.assertRaises(ValueError):
            adme_schema.adme_auth_method("invalid")

        values = {"adme_auth_method": "SP", "adme_tenant_id": "tenant-id"}
        self.assertEqual(adme_schema.adme_auth_value("ADME_TENANT_ID", values), "tenant-id")
        funcs["_adme_auth_value"].__globals__.update(values)
        self.assertEqual(funcs["_adme_auth_value"]("ADME_TENANT_ID"), "tenant-id")
        self.assertEqual(adme_schema.adme_authority_url("tenant-id"), funcs["_adme_authority_url"]())
        funcs["_adme_managed_identity_client_id"].__globals__["adme_managed_identity_client_id"] = " client-id "
        self.assertEqual(adme_schema.adme_managed_identity_client_id(" client-id "), funcs["_adme_managed_identity_client_id"]())
        self.assertEqual(adme_schema.adme_keyvault_url("contoso-kv"), funcs["_adme_keyvault_url"]("contoso-kv"))
        self.assertEqual(
            adme_schema.schema_doc_cache_key("kind", "https://contoso.energy.azure.com", "data"),
            funcs["_schema_doc_cache_key"]("kind"),
        )

        kind = "osdu:wks:reference-data--DurationContext:1.0.0"
        schema_url = adme_schema.adme_schema_url(kind, "https://contoso.energy.azure.com")
        self.assertEqual(schema_url, funcs["_adme_schema_url"](kind))
        self.assertEqual(
            adme_schema.adme_schema_source_prefix("https://contoso.energy.azure.com", "data"),
            funcs["_adme_schema_source_prefix"](),
        )
        self.assertEqual(
            adme_schema.adme_schema_source(schema_url, "https://contoso.energy.azure.com", "data"),
            funcs["_adme_schema_source"](schema_url),
        )
        self.assertEqual(
            adme_schema.adme_schema_list_url("https://contoso.energy.azure.com", 0),
            funcs["_adme_schema_list_url"](0),
        )

        class Response:
            text = " detail\nline "

        self.assertEqual(adme_schema.adme_response_detail(Response()), funcs["_adme_response_detail"](Response()))
        self.assertEqual(adme_schema.adme_schema_timeout(), funcs["_adme_schema_timeout"]())
        self.assertEqual(adme_schema.adme_schema_retry_status_codes(), funcs["_adme_schema_retry_status_codes"]())
        self.assertEqual(adme_schema.adme_schema_retry_summary(), funcs["_adme_schema_retry_summary"]())


if __name__ == "__main__":
    unittest.main()
