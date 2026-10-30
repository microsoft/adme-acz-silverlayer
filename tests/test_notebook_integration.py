"""End-to-end notebook runs on local Spark/Delta.

Tests use synthetic bronze rows and a stubbed ADME schema service and token.
No Azure login or deployed services are required.
"""

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

from notebook_runner import run_notebook
from test_normalization import T, java_is_available
from tno_bronze import BRONZE_DDL

if T is not None:
    from pyspark.sql import SparkSession

INTEGRATION_DEPENDENCIES_AVAILABLE = all(
    importlib.util.find_spec(name) is not None
    for name in ("delta", "requests", "msal", "azure", "pandas", "pyarrow", "cryptography")
) and importlib.util.find_spec("azure.identity") is not None

WELL_KIND = "osdu:wks:master-data--Well:1.0.0"
WELL_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Well",
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "kind": {"type": "string"},
        "data": {
            "allOf": [{
                "type": "object",
                "properties": {
                    "FacilityName": {"type": "string"},
                    "FacilityTypeID": {
                        "type": "string",
                        "x-osdu-relationship": [{"GroupType": "reference-data", "EntityType": "FacilityType"}],
                    },
                    "NameAliases": {
                        "type": "array",
                        "items": {"type": "object", "properties": {"AliasName": {"type": "string"}}},
                    },
                },
            }],
        },
    },
}

def bronze_row(record_id, version, payload, active=True, kind=WELL_KIND, modify_time=1760000000000):
    return (
        json.dumps({"data": payload, "meta": None, "modifyUser": "buildagent", "modifyTime": modify_time}), None, record_id, version, kind, None, None, None,
        "buildagent", None, None, None, None, None, None, None, active,
    )


def build_session(warehouse: str):
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[2]").appName("notebook-integration")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@unittest.skipIf(T is None or not INTEGRATION_DEPENDENCIES_AVAILABLE,
                 "Install optional [integration] dependencies for notebook integration tests.")
class NotebookIntegrationBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not java_is_available():
            raise unittest.SkipTest("A working Java runtime is required for notebook integration tests.")
        cls.temporary = tempfile.TemporaryDirectory(prefix="notebook-integration-")
        cls.addClassCleanup(cls.temporary.cleanup)
        environment = mock.patch.dict(os.environ, {
            "PYSPARK_PYTHON": sys.executable,
            "SPARK_LOCAL_IP": "127.0.0.1",
            "ADME_SPARK_SHUFFLE_PARTITIONS": "2",
            "ADME_WORKSPACE_ID": "00000000-0000-0000-0000-000000000001",
            "ADME_LAKEHOUSE_ID": "00000000-0000-0000-0000-000000000002",
        })
        environment.start()
        cls.addClassCleanup(environment.stop)
        cls.spark = build_session(cls.temporary.name + "/warehouse")
        cls.addClassCleanup(cls.spark.stop)

    def read(self, table):
        return self.spark.table(table)


class OfflineNotebookRunTests(NotebookIntegrationBase):
    PREFIX = "it_"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.createDataFrame([
            bronze_row("test:well:1", "1", {"FacilityName": "Alpha", "FacilityTypeID": "test:reference-data--FacilityType:Well:",
                                            "NameAliases": [{"AliasName": "A1"}, {"AliasName": "A2"}]}),
            bronze_row("test:well:2", "1", {"FacilityName": "Beta", "NameAliases": []}),
            bronze_row("test:reference-data--FacilityType:Well", "1", {"Name": "Well"},
                       kind="osdu:wks:reference-data--FacilityType:1.0.0"),
            bronze_row("test:well:3", "1", {"FacilityName": "Retired"}, active=False),
        ], BRONZE_DDL).write.format("delta").saveAsTable("osducatalog")

        schema_urls = []

        def serve_schema(url, context, timeout=None, session=None):
            schema_urls.append(url)
            return [] if "latestVersion" in url else {"schema": WELL_SCHEMA}

        def stub_services(namespace):
            namespace["get_adme_access_token"] = lambda: "unit-test-token"
            namespace["_adme_schema_get_json"] = serve_schema
            namespace["_silver_write_batches"] = []
            write_silver_tables = namespace["write_silver_tables"]

            def capture_write_batch(writes, mode="overwrite", parallelism=None, replace_where=None):
                namespace["_silver_write_batches"].append([target for _, target in writes])
                return write_silver_tables(
                    writes, mode=mode, parallelism=parallelism, replace_where=replace_where,
                )

            namespace["write_silver_tables"] = capture_write_batch

        cls.displayed = []
        cls.schema_urls = schema_urls
        cls.namespace = run_notebook(cls.spark, {
            "ADME_ENDPOINT": "https://adme.example.test",
            "ADME_DATA_PARTITION_ID": "test-partition",
            "ADME_AUTH_METHOD": "MI",
            "RUN_PROFILE": "execute",
            "KINDS": [WELL_KIND],
            "TABLE_PREFIX": cls.PREFIX,
            "ALLOW_OVERWRITE": True,
            "WRITE_MODE": "full_refresh",
            "VERSION_STRATEGY": "merge",
            "OUTPUT_WRITE_PARALLELISM": 2,
        }, before_pipeline=stub_services, displayed=cls.displayed)

    def test_schema_service_was_queried_for_the_selected_kind(self):
        self.assertTrue(any(url.endswith("/" + WELL_KIND) for url in self.schema_urls))

    def test_parent_table_contains_only_active_records(self):
        rows = {row["id"]: row for row in self.read("it_osdu_wks_well").collect()}
        self.assertEqual({"test:well:1", "test:well:2"}, set(rows))
        self.assertEqual("Alpha", rows["test:well:1"]["data__FacilityName"])

    def test_wrapper_epoch_milliseconds_are_normalized_to_timestamp(self):
        normalized = self.namespace["_normalize_bronze_record_wrapper"](self.read("osducatalog"))
        row = normalized.where("id = 'test:well:1'").selectExpr("unix_timestamp(modifyTime) AS epoch_seconds").first()
        self.assertEqual(1760000000, row["epoch_seconds"])

    def test_invalid_wrapper_timestamp_does_not_abort_normalization(self):
        record = bronze_row("test:well:invalid-time", "1", {"FacilityName": "Invalid"}, modify_time="0")
        source = self.spark.createDataFrame([record], BRONZE_DDL)
        normalized = self.namespace["_normalize_bronze_record_wrapper"](source)
        self.assertIsNone(normalized.select("modifyTime").first()["modifyTime"])

    def test_array_elements_become_child_rows_keyed_by_parent(self):
        aliases = self.read("it_osdu_wks_well___namealiases").collect()
        self.assertEqual({"A1", "A2"}, {row["AliasName"] for row in aliases})
        self.assertEqual({"test:well:1"}, {row["id"] for row in aliases})

    def test_declared_relationship_is_published_to_a_bridge_table(self):
        bridge = [name for name in (t.name for t in self.spark.catalog.listTables())
                  if self.namespace["_is_relationship_bridge"](name, "it_osdu_wks_well")]
        self.assertEqual(1, len(bridge), bridge)
        self.assertEqual(bridge[0], "it_osdu_wks_well__facilitytypeid__reference_data_facilitytype")
        row = self.read(bridge[0]).collect()[0]
        self.assertEqual(("test:well:1", "resolved", "test:reference-data--FacilityType:Well"),
                         (row["source_record_key"].rsplit(":", 1)[0], row["status"], row["target_record_key"].rsplit(":", 1)[0]))
        self.assertEqual(self.read(bridge[0]).columns, [
            "source_record_key", "target_record_key", "source_kind", "status", "run_id",
        ])
        self.assertTrue(all(field.dataType.simpleString() == "string" for field in self.read(bridge[0]).schema.fields))
        self.assertEqual(
            row["source_record_key"], self.read("it_osdu_wks_well").where("id = 'test:well:1'").first()["record_key"],
        )

    def test_relationship_bridges_use_the_parallel_write_batch(self):
        self.assertTrue(
            any(
                any(self.namespace["_is_relationship_bridge"](target, "it_osdu_wks_well") for target in batch)
                for batch in self.namespace["_silver_write_batches"]
            )
        )

    def test_run_metadata_reports_success(self):
        statuses = {row["status"] for row in self.read("silver_run_status").collect()}
        self.assertEqual({"started", "committed"}, {s.lower() for s in statuses})
        self.assertEqual(1, len([r for r in self.namespace["results"] if r]))
        self.assertGreaterEqual(self.read("silver_run_manifest").count(), 1)

    def test_results_summary_is_displayed(self):
        self.assertTrue(self.displayed)
        self.assertEqual(self.namespace["df_results"]["Children"].tolist(), [1])
        self.assertEqual(self.namespace["df_results"]["Bridges"].tolist(), [1])
        bridge_rows = self.namespace["df_bridges"].to_dict("records")
        self.assertEqual(len(bridge_rows), 1)
        self.assertEqual(bridge_rows[0]["Source kinds"], WELL_KIND)
        self.assertEqual(bridge_rows[0]["Parent tables"], "it_osdu_wks_well")
        self.assertEqual(bridge_rows[0]["Source statuses"], "success")
        self.assertTrue(self.spark.catalog.tableExists(bridge_rows[0]["Bridge table"]))


if __name__ == "__main__":
    unittest.main()
