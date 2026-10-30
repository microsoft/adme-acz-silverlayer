"""Schema-aware OneLake paths using mocked Fabric metadata and execution credentials."""

import copy
import importlib.util
import json
import logging
import re
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import urlsplit

from test_runtime_boundary_modules import extract_functions
from adme_acz_silverlayer import fabric_io


WORKSPACE = "11111111-1111-4111-8111-111111111111"
LAKEHOUSE = "22222222-2222-4222-8222-222222222222"
OTHER_LAKEHOUSE = "33333333-3333-4333-8333-333333333333"


class _LakehouseLayoutTestCase(unittest.TestCase):
    def setUp(self):
        functions = extract_functions([
            "_lakehouse_default_schema", "_lakehouse_table_path",
            "_fabric_lakehouse_layout", "_onelake_table_uri", "_table_path_uri",
            "_requires_path_fallback", "_write_table", "_read_target_df_for_merge",
            "_read_table_for_cache", "read_bronze_table_spark",
        ])
        self.resolve = functions["_fabric_lakehouse_layout"]
        self.uri = functions["_onelake_table_uri"]
        self.output_uri = functions["_table_path_uri"]
        self.functions = functions
        self.metadata = {
            "id": LAKEHOUSE, "workspaceId": WORKSPACE, "type": "Lakehouse",
            "properties": {"defaultSchema": "curated"},
        }
        self.response = Mock(status_code=200)
        self.response.json.side_effect = lambda: copy.deepcopy(self.metadata)
        self.session = Mock()
        self.session.get.return_value = self.response
        session_context = Mock()
        session_context.__enter__ = Mock(return_value=self.session)
        session_context.__exit__ = Mock(return_value=False)
        self.credentials = Mock()
        self.credentials.getToken.return_value = "synthetic-token"
        self.conf = Mock()
        self.conf.get.return_value = "https://onelake.dfs.fabric.microsoft.com/"
        self.logger = Mock(spec=logging.Logger)
        self.resolve.__globals__.update({
            "uuid": uuid, "re": re, "urlsplit": urlsplit,
            "_FABRIC_LAKEHOUSE_LAYOUTS": {},
            "_FABRIC_LAKEHOUSE_LAYOUT_LOCK": threading.RLock(),
            "notebookutils": SimpleNamespace(credentials=self.credentials, conf=self.conf),
            "requests": SimpleNamespace(Session=Mock(return_value=session_context)),
            "HTTPAdapter": Mock(), "Retry": Mock(), "logger": self.logger,
            "workspace_id": WORKSPACE, "lakehouse_id": LAKEHOUSE,
        })


class LakehouseLayoutTests(_LakehouseLayoutTestCase):
    def test_default_schema_and_explicit_schema_paths(self):
        self.assertEqual(self.uri(WORKSPACE, LAKEHOUSE, "silver"),
                         f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}/Tables/curated/silver")
        self.assertEqual(self.output_uri("bronze.osducatalog"),
                         f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}/Tables/bronze/osducatalog")
        self.assertEqual(self.uri(WORKSPACE, LAKEHOUSE, "silver"),
                         fabric_io.onelake_table_uri(WORKSPACE, LAKEHOUSE, "silver", "curated"))
        self.credentials.getToken.assert_called_once_with("pbi")
        self.session.get.assert_called_once_with(
            f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE}/lakehouses/{LAKEHOUSE}",
            headers={"Authorization": "Bearer synthetic-token", "x-ms-fabric-skill": "spark-cli"},
            timeout=30, allow_redirects=False,
        )

    def test_schema_less_layout_and_explicit_schema_requirement(self):
        self.metadata["properties"] = {}
        self.assertEqual(self.output_uri("silver"),
                         f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}/Tables/silver")
        with self.assertRaisesRegex(ValueError, "schema-enabled"):
            self.output_uri("dbo.silver")
        self.session.get.assert_called_once()

    def test_successful_cache_is_scoped_to_destination_and_shared_by_threads(self):
        with ThreadPoolExecutor(max_workers=4) as executor:
            paths = list(executor.map(lambda _: self.output_uri("silver"), range(12)))
        self.assertEqual(len(set(paths)), 1)
        self.session.get.assert_called_once()
        self.metadata["id"] = OTHER_LAKEHOUSE
        self.metadata["properties"]["defaultSchema"] = "other"
        self.assertIn("/Tables/other/silver", self.uri(WORKSPACE, OTHER_LAKEHOUSE, "silver"))
        self.assertEqual(self.session.get.call_count, 2)
        self.assertIn("/Tables/curated/silver", self.output_uri("silver"))

    def test_failed_lookup_is_not_cached_and_can_recover(self):
        self.response.status_code = 403
        with self.assertRaisesRegex(RuntimeError, "refusing to guess"):
            self.output_uri("silver")
        self.logger.exception.assert_called_once()
        self.response.status_code = 200
        self.assertIn("/Tables/curated/silver", self.output_uri("silver"))
        self.assertEqual(self.session.get.call_count, 2)

    def test_http_errors_never_mean_schema_less(self):
        for status in (301, 401, 403, 404, 429, 500):
            with self.subTest(status=status):
                self.response.status_code = status
                with self.assertRaisesRegex(RuntimeError, "refusing to guess"):
                    self.output_uri("silver")
        self.response.json.assert_not_called()

    def test_token_network_and_json_failures_propagate(self):
        for target in (self.credentials.getToken, self.session.get, self.response.json):
            with self.subTest(target=target):
                target.side_effect = RuntimeError("synthetic failure")
                with self.assertRaisesRegex(RuntimeError, "refusing to guess"):
                    self.output_uri("silver")
                target.side_effect = None

    def test_malformed_metadata_is_not_schema_less(self):
        invalid = [
            None, [], {}, {**self.metadata, "type": "Warehouse"},
            {**self.metadata, "id": OTHER_LAKEHOUSE},
            {**self.metadata, "workspaceId": OTHER_LAKEHOUSE},
            {**self.metadata, "id": None},
            {key: value for key, value in self.metadata.items() if key != "properties"},
            {**self.metadata, "properties": None},
            {**self.metadata, "properties": []},
        ]
        invalid.extend({**self.metadata, "properties": {"defaultSchema": value}}
                       for value in (None, "", " ", 12, [], "../dbo", "dbo/other"))
        for metadata in invalid:
            with self.subTest(metadata=metadata):
                self.response.json.side_effect = lambda: metadata
                with self.assertRaisesRegex(RuntimeError, "refusing to guess"):
                    self.output_uri("silver")

    def test_metadata_parser_is_identical_in_package_and_notebook(self):
        parse = self.resolve.__globals__["_lakehouse_default_schema"]
        for schema in ("dbo", "custom_schema", None):
            metadata = copy.deepcopy(self.metadata)
            metadata["properties"] = {} if schema is None else {"defaultSchema": schema}
            self.assertEqual(parse(metadata, WORKSPACE, LAKEHOUSE),
                             fabric_io._lakehouse_default_schema(metadata, WORKSPACE, LAKEHOUSE))

    def test_invalid_destination_ids_fail_before_credentials(self):
        for workspace, lakehouse in (("", LAKEHOUSE), (WORKSPACE, "lakehouse-name"), (None, LAKEHOUSE)):
            with self.subTest(workspace=workspace, lakehouse=lakehouse):
                with self.assertRaisesRegex(ValueError, "UUIDs"):
                    self.uri(workspace, lakehouse, "silver")
        self.credentials.getToken.assert_not_called()

    def test_table_paths_cannot_escape_or_duplicate_schema(self):
        for schema in ("dbo", None):
            for table in ("../silver", "/silver", "a/b", "a.b.c", "", "a..b", "a?b", "a#b", "a%2fb"):
                with self.subTest(schema=schema, table=table), self.assertRaises(ValueError):
                    fabric_io._lakehouse_table_path(table, schema)
        self.assertEqual(fabric_io._lakehouse_table_path("dbo.silver", "dbo"), "dbo/silver")

    def test_invalid_token_or_endpoint_fails_before_http(self):
        self.credentials.getToken.return_value = ""
        with self.assertRaises(RuntimeError):
            self.output_uri("silver")
        self.credentials.getToken.return_value = "synthetic-token"
        for endpoint in (None, "", "http://example.com", "https://user:pass@example.com", "host/path"):
            self.conf.get.return_value = endpoint
            with self.subTest(endpoint=endpoint), self.assertRaises(RuntimeError):
                self.output_uri("silver")
        self.session.get.assert_not_called()

    def test_bronze_output_merge_and_metadata_share_resolved_path(self):
        spark = Mock()
        spark.catalog.tableExists.return_value = False
        spark.table.side_effect = RuntimeError("No default context found")
        reader = Mock()
        spark.read.format.return_value = reader
        frame = Mock()
        writer = Mock()
        frame.write.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer
        writer.saveAsTable.side_effect = RuntimeError("No default context found")
        self.resolve.__globals__.update({
            "spark": spark,
            "_normalize_bronze_record_wrapper": lambda df: df,
            "apply_active_record_filter": lambda df: df,
        })
        root = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}/Tables/curated"
        self.functions["read_bronze_table_spark"](spark, WORKSPACE, LAKEHOUSE, "osducatalog")
        reader.load.assert_called_with(f"{root}/osducatalog")
        self.functions["_write_table"](frame, "silver")
        writer.save.assert_called_once_with(f"{root}/silver")
        self.functions["_read_target_df_for_merge"](spark, "silver", by_path=True)
        reader.load.assert_called_with(f"{root}/silver")
        self.functions["_read_table_for_cache"]("silver_run_manifest")
        reader.load.assert_called_with(f"{root}/silver_run_manifest")
        self.session.get.assert_called_once()

    def test_failed_metadata_lookup_never_writes_a_guessed_path(self):
        frame = Mock()
        writer = Mock()
        frame.write.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer
        writer.saveAsTable.side_effect = RuntimeError("No default context found")
        self.response.status_code = 403
        with self.assertRaisesRegex(RuntimeError, "refusing to guess"):
            self.functions["_write_table"](frame, "silver")
        writer.save.assert_not_called()


@unittest.skipUnless(importlib.util.find_spec("requests"), "requests is not installed")
class LakehouseLayoutHttpTests(_LakehouseLayoutTestCase):
    def setUp(self):
        super().setUp()
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        self.replies = []
        self.received = []
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                fixture.received.append((self.path, dict(self.headers)))
                status, headers, body = fixture.replies.pop(0) if fixture.replies else (500, {}, {})
                encoded = json.dumps(body).encode()
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()

        def stop_server():
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())

        self.addCleanup(stop_server)
        self.local_url = f"http://127.0.0.1:{server.server_port}"
        api_path = f"/v1/workspaces/{WORKSPACE}/lakehouses/{LAKEHOUSE}"

        class LoopbackAdapter(HTTPAdapter):
            def send(self, request, **kwargs):
                # Only the destination changes; Requests/urllib3 perform the real retries.
                fixture.assertEqual(request.url, "https://api.fabric.microsoft.com" + api_path)
                request.url = fixture.local_url + api_path
                return super().send(request, **kwargs)

        def session_factory():
            session = requests.Session()
            session.trust_env = False
            return session

        self.resolve.__globals__.update({
            "requests": SimpleNamespace(Session=session_factory),
            "HTTPAdapter": LoopbackAdapter,
            "Retry": Retry,
        })
        sleep_patch = patch("urllib3.util.retry.time.sleep")
        self.sleep = sleep_patch.start()
        self.addCleanup(sleep_patch.stop)

    def test_throttling_honors_retry_after_and_recovers(self):
        self.replies = [
            (429, {"Retry-After": "2"}, {}),
            (503, {}, {}),
            (200, {}, self.metadata),
        ]
        path = self.output_uri("silver")
        self.assertIn("/Tables/curated/silver", path)
        self.assertEqual(len(self.received), 3)
        self.assertEqual([call.args[0] for call in self.sleep.call_args_list], [2, 2.0])
        for request_path, headers in self.received:
            self.assertEqual(request_path, f"/v1/workspaces/{WORKSPACE}/lakehouses/{LAKEHOUSE}")
            self.assertEqual(headers["Authorization"], "Bearer synthetic-token")
            self.assertEqual(headers["x-ms-fabric-skill"], "spark-cli")
        self.assertEqual(self.output_uri("silver"), path)
        self.assertEqual(len(self.received), 3)

    def test_retry_exhaustion_fails_without_caching_then_recovers(self):
        self.replies = [(503, {}, {}) for _ in range(4)]
        with self.assertRaisesRegex(RuntimeError, "refusing to guess"):
            self.output_uri("silver")
        self.assertEqual(len(self.received), 4)
        self.assertEqual([call.args[0] for call in self.sleep.call_args_list], [2.0, 4.0])
        self.logger.exception.assert_called_once()
        self.replies = [(200, {}, {**self.metadata, "properties": {}})]
        self.assertTrue(self.output_uri("silver").endswith("/Tables/silver"))
        self.assertEqual(len(self.received), 5)

    def test_authorization_and_not_found_fail_without_retries(self):
        for status in (401, 403, 404):
            with self.subTest(status=status):
                count = len(self.received)
                self.replies = [(status, {}, {})]
                with self.assertRaisesRegex(RuntimeError, "refusing to guess"):
                    self.output_uri("silver")
                self.assertEqual(len(self.received), count + 1)
        self.sleep.assert_not_called()

    def test_redirect_is_not_followed(self):
        self.replies = [(302, {"Location": self.local_url + "/redirect-target"}, {})]
        with self.assertRaisesRegex(RuntimeError, "refusing to guess"):
            self.output_uri("silver")
        self.assertEqual(len(self.received), 1)
        self.sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
