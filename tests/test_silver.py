"""Real local Spark transformations and injected publication failures."""

import copy
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from test_normalization import ROOT, T, java_is_available
from tno_bronze import BRONZE_DDL, bronze_row

if T is not None:
    from pyspark.sql import SparkSession, functions as F
    from adme_acz_silverlayer.silver import build_silver, mark_latest, release_silver
    from adme_acz_silverlayer.silver_publish import _schema_signature, publish_silver


ASSET = "example:wks:master-data--Asset:1.0.0"
ASSET_REVISION = "example:wks:master-data--Asset:2.0.0"
SURVEY = "example:wks:work-product--Survey:1.0.0"
ASSET_ID = "test:master-data--Asset:a"
OTHER_ID = "test:master-data--Asset:b"
SURVEY_ID = "test:work-product--Survey:c"
LARGE_VERSION = "100000000000000000000000000000001"


def synthetic_schemas():
    reference = {"type": "string", "x-osdu-relationship": [{"GroupType": "master-data", "EntityType": "Asset"}]}
    schemas = {}
    for kind in (ASSET, ASSET_REVISION, SURVEY):
        fields = {"Name": {"type": "string"}}
        if kind == SURVEY:
            fields.update({
                "Target": reference, "UnannotatedID": {"type": "string"},
                "Duplicates": {"type": "array", "items": reference},
                "Groups": {"type": "array", "items": {
                    "type": "object", "properties": {"Links": {"type": "array", "items": reference}},
                }},
            })
        schemas[kind] = {"x-osdu-schema-source": kind, "type": "object", "properties": {
            "data": {"type": "object", "properties": fields},
            "createTime": {"type": "string", "format": "date-time"},
        }}
    return schemas


class MemoryRunStore:
    """I/O injection only: this does not emulate or prove Delta transactions."""

    def __init__(self, *, failure=None):
        self.failure = failure
        self.frames = {}
        self.events = {}
        self.claimed = set()

    def claim(self, path):
        if path in self.claimed:
            raise FileExistsError("Run already claimed")
        self.claimed.add(path)

    def qualify(self, path):
        return path

    def journal(self, path, event):
        if self.failure == "journal" and path.endswith("started.json"):
            raise OSError("Injected journal failure")
        self.events[path] = copy.deepcopy(event)

    def write(self, frame, path):
        if self.failure == "write" and self.frames:
            raise OSError("Injected output failure")
        if path in self.frames:
            raise FileExistsError("Output exists")
        self.frames[path] = frame
        return 0

    def read(self, path, version):
        if self.failure == "read":
            raise OSError("Injected read-back failure")
        if self.failure == "rows":
            return self.frames[path].limit(0)
        if self.failure == "schema":
            return self.frames[path].drop(self.frames[path].columns[-1])
        return self.frames[path]

    def version(self, path):
        return 1 if self.failure == "concurrent" else 0


@unittest.skipIf(T is None, "Install the optional [spark] dependency for Silver integration tests.")
class SilverIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not java_is_available():
            raise unittest.SkipTest("A working Java runtime is required for local Spark tests.")
        cls.temporary = tempfile.TemporaryDirectory(prefix="silver-reference-tests-")
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.environment = mock.patch.dict(os.environ, {
            "PYSPARK_PYTHON": sys.executable,
            "SPARK_LOCAL_IP": "127.0.0.1",
            "PYTHONPATH": str(ROOT / "src") + os.pathsep + os.environ.get("PYTHONPATH", ""),
        })
        cls.environment.start()
        cls.addClassCleanup(cls.environment.stop)
        cls.spark = SparkSession.builder.master("local[2]").appName("synthetic-silver-reference").config(
            "spark.ui.enabled", "false"
        ).config("spark.driver.host", "127.0.0.1").config(
            "spark.driver.bindAddress", "127.0.0.1"
        ).config("spark.sql.shuffle.partitions", "2").config(
            "spark.sql.warehouse.dir", cls.temporary.name
        ).getOrCreate()
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")
        schema = T.StructType([
            T.StructField("id", T.StringType()), T.StructField("version", T.StringType()),
            T.StructField("kind", T.StringType()), T.StructField("data", T.StringType()),
            T.StructField("isActive", T.BooleanType()),
            T.StructField("extra", T.MapType(T.StringType(), T.StringType())),
            T.StructField("opaque", T.StructType([T.StructField("code", T.StringType())])),
            T.StructField("createTime", T.TimestampType()),
        ])
        rows = [
            (ASSET_ID, "0009", ASSET, {"Name": "available"}, True),
            (ASSET_ID, LARGE_VERSION, ASSET_REVISION, {"Name": "deleted latest"}, False),
            (OTHER_ID, "0", ASSET, {"Name": "other"}, True),
            (SURVEY_ID, "1", SURVEY, {
                "Target": ASSET_ID + ":0009", "UnannotatedID": ASSET_ID + ":",
                "Duplicates": [ASSET_ID + ":", ASSET_ID + ":", None, ASSET_ID + ":88",
                               "test:reference-data--Other:x:", "test:master-data--Asset:missing:"],
                "Groups": [{"Links": [ASSET_ID + ":9", None]}, {"Links": [OTHER_ID + ":"]}, None],
            }, False),
            (SURVEY_ID, "2", SURVEY, {"Target": ASSET_ID + ":88", "Groups": [], "Duplicates": None}, True),
        ]
        cls.source = cls.spark.createDataFrame([
            (*row[:3], json.dumps(row[3]), row[4], {"b": "2", "a": "1"}, {"code": "preserved"},
             datetime(2025, 1, 1, 12, 34, 56, 123456))
            for row in rows
        ], schema)
        cls.result = build_silver(cls.source, synthetic_schemas(), "fixture-run")
        cls.addClassCleanup(release_silver, cls.result)
        cls.bridge = cls.result["outputs"]["gen_silver_osdu_relationships"].collect()

    def test_all_versions_and_deleted_state_across_schema_tables(self):
        roots = [frame for name, frame in self.result["outputs"].items()
                 if "___" not in name and name != "gen_silver_osdu_relationships"]
        rows = [row.asDict() for frame in roots for row in frame.collect()]
        self.assertEqual(len(rows), self.source.count())
        asset = [row for row in rows if row["id"] == ASSET_ID]
        self.assertEqual({row["version"]: row["_silver_is_latest"] for row in asset},
                         {"0009": False, LARGE_VERSION: True})
        self.assertFalse(next(row for row in asset if row["_silver_is_latest"])["isActive"])
        self.assertTrue(all(row["opaque"]["code"] == "preserved" for row in rows))
        self.assertTrue(all(row["extra"] == {"a": "1", "b": "2"} for row in rows))
        self.assertTrue(all(row["createTime"] == datetime(2025, 1, 1, 12, 34, 56, 123456) for row in rows))
        self.assertTrue(all(row["osdu__createTime"].endswith("123456Z") for row in rows))

    def test_exact_latest_missing_invalid_and_null_references(self):
        statuses = {row.status for row in self.bridge}
        self.assertEqual(statuses, {"resolved", "target_deleted", "version_not_found", "invalid_reference",
                                    "target_not_loaded", "absent"})
        exact = next(row for row in self.bridge if row.raw_reference == ASSET_ID + ":0009")
        self.assertEqual((exact.target_version, exact.status), ("0009", "resolved"))
        latest = [row for row in self.bridge if row.raw_reference == ASSET_ID + ":"]
        self.assertEqual(len(latest), 2)
        self.assertTrue(all(row.target_version == LARGE_VERSION and row.status == "target_deleted" for row in latest))
        self.assertTrue(all(row.target_id is None for row in self.bridge if row.status == "version_not_found"))
        self.assertFalse(any("UnannotatedID" in row.source_path for row in self.bridge))

    def test_acz_wrappers_without_embedded_identity_build_typed_fields_and_bridges(self):
        rows = [
            bronze_row({"id": ASSET_ID, "kind": ASSET, "data": {"Name": "Asset"}}, "test"),
            bronze_row({"id": SURVEY_ID, "kind": SURVEY,
                        "data": {"Name": "Survey", "Target": ASSET_ID + ":"}}, "test"),
        ]
        source = self.spark.createDataFrame(rows, BRONZE_DDL)
        result = build_silver(source, synthetic_schemas(), "acz-wrapper")
        try:
            parents = [frame for name, frame in result["outputs"].items()
                       if "___" not in name and name != "gen_silver_osdu_relationships"]
            projected = {record.id: record for frame in parents for record in frame.collect()}
            self.assertEqual(projected[ASSET_ID]["osdu__data__Name"], "Asset")
            self.assertEqual(projected[SURVEY_ID]["osdu__data__Name"], "Survey")
            self.assertEqual(projected[SURVEY_ID]["osdu__data__Target"], ASSET_ID + ":")
            self.assertEqual(projected[SURVEY_ID]["data"], rows[1][0])
            bridge = result["outputs"]["gen_silver_osdu_relationships"].collect()
            self.assertEqual([(edge.source_id, edge.target_id, edge.status) for edge in bridge],
                             [(SURVEY_ID, ASSET_ID, "resolved")])
        finally:
            release_silver(result)

    def test_nested_array_ordinals_and_latest_flags(self):
        name = next(name for name in self.result["outputs"] if name.endswith("___groups__items__links"))
        rows = self.result["outputs"][name].orderBy("_silver_ordinal_path").collect()
        self.assertEqual([row._silver_ordinal_path for row in rows], [[0, 0], [0, 1], [1, 0]])
        self.assertEqual([row.value for row in rows], [ASSET_ID + ":9", None, OTHER_ID + ":"])
        self.assertTrue(all(not row._silver_is_latest and row.version == "1" for row in rows))
        self.assertTrue(all(not row.isActive for row in rows))
        group = next(name for name in self.result["outputs"] if name.endswith("___groups"))
        self.assertEqual(self.result["outputs"][group].where("_silver_element_is_null").count(), 1)

    def test_publication_success_is_last_and_immutable_paths_are_recorded(self):
        store = MemoryRunStore()
        event = publish_silver(self.result, {"source_path": "Tables/source", "source_delta_version": 12},
                               "Tables", "Files/journal", store)
        self.assertEqual(event["status"], "succeeded")
        self.assertEqual(event["uncommitted_outputs"], [])
        self.assertTrue(next(reversed(store.events)).endswith("/succeeded.json"))
        self.assertTrue(all(path.endswith("__run_fixture_run") for path in event["output_paths"].values()))
        with self.assertRaises(FileExistsError):
            publish_silver(self.result, {"source_path": "Tables/source", "source_delta_version": 12},
                           "Tables", "Files/journal", store)

    def test_publication_failures_do_not_produce_a_success_marker(self):
        for failure in ("journal", "write", "read", "schema", "rows"):
            with self.subTest(failure=failure), self.assertLogs("adme_silver_reference", level="ERROR"):
                store = MemoryRunStore(failure=failure)
                with self.assertRaises((OSError, ValueError)):
                    publish_silver(self.result, {"source_path": "Tables/source", "source_delta_version": 12},
                                   "Tables", "Files/journal", store)
                self.assertFalse(any(path.endswith("succeeded.json") for path in store.events))
                failed = next(event for path, event in store.events.items() if path.endswith("failed.json"))
                self.assertEqual(failed["status"], "failed")
                if failure == "write":
                    self.assertEqual(len(failed["committed_outputs"]), 1)
                    self.assertEqual(len(failed["attempted_outputs"]), 2)
                    self.assertTrue(failed["uncommitted_outputs"])

    def test_concurrent_output_changes_are_rejected(self):
        store = MemoryRunStore(failure="concurrent")
        with self.assertLogs("adme_silver_reference", level="ERROR"), self.assertRaisesRegex(ValueError, "Concurrent"):
            publish_silver(self.result, {"source_path": "Tables/source", "source_delta_version": 12},
                           "Tables", "Files/journal", store)
        self.assertFalse(any(path.endswith("succeeded.json") for path in store.events))

    def test_missing_schemas_empty_input_and_ambiguous_versions_fail(self):
        with self.assertRaisesRegex(ValueError, "Exact schemas"):
            build_silver(self.source, {}, "missing")
        with self.assertRaisesRegex(ValueError, "no kinds"):
            build_silver(self.source.limit(0), {}, "empty")
        duplicate = self.source.where(F.col("version") == "0009").withColumn("version", F.lit("9"))
        with self.assertRaisesRegex(ValueError, "Ambiguous"):
            mark_latest(self.source.unionByName(duplicate))

    def test_exact_integer_versions_are_supported_but_floats_are_rejected(self):
        source = self.source.where(F.col("version") == "0009").withColumn("version", F.lit(9).cast("long"))
        row = mark_latest(source).first()
        self.assertEqual(row._silver_version_key, "9")
        self.assertEqual(row.version, 9)
        with self.assertRaisesRegex(ValueError, "exact integer"):
            mark_latest(source.withColumn("version", F.col("version").cast("double")))

    def test_readback_ignores_nullability_but_preserves_nested_metadata(self):
        def schema(nullable, metadata):
            return T.StructType([T.StructField("value", T.StringType(), nullable=nullable, metadata=metadata)])

        metadata = {"nested": {"nullable": "annotation", "containsNull": "annotation"}}
        expected = _schema_signature(schema(False, metadata).jsonValue())
        self.assertEqual(expected, _schema_signature(schema(True, metadata).jsonValue()))
        self.assertNotEqual(expected, _schema_signature(schema(True, {"nested": {"nullable": "different"}}).jsonValue()))


if __name__ == "__main__":
    unittest.main()
