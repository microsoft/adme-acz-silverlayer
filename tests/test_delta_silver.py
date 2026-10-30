"""Optional real Delta round-trip with synthetic data and local filesystem I/O."""

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test_normalization import ROOT, T, java_is_available

if T is not None:
    from py4j.protocol import Py4JJavaError
    from pyspark.sql import SparkSession
    from adme_acz_silverlayer.silver import build_silver, release_silver
    from adme_acz_silverlayer.silver_publish import read_pinned_source
    from adme_acz_silverlayer.silver_publish import publish_silver


@unittest.skipIf(T is None or importlib.util.find_spec("delta") is None,
                 "Install optional [delta] dependencies for real Delta publication tests.")
class DeltaSilverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not java_is_available():
            raise unittest.SkipTest("A working Java runtime is required for local Delta tests.")
        from delta import configure_spark_with_delta_pip

        cls.temporary = tempfile.TemporaryDirectory(prefix="synthetic-delta-silver-")
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.environment = mock.patch.dict(os.environ, {
            "PYSPARK_PYTHON": sys.executable,
            "SPARK_LOCAL_IP": "127.0.0.1",
            "PYTHONPATH": str(ROOT / "src") + os.pathsep + os.environ.get("PYTHONPATH", ""),
        })
        cls.environment.start()
        cls.addClassCleanup(cls.environment.stop)
        builder = SparkSession.builder.master("local[2]").appName("synthetic-delta-silver").config(
            "spark.ui.enabled", "false"
        ).config("spark.driver.host", "127.0.0.1").config(
            "spark.driver.bindAddress", "127.0.0.1"
        ).config("spark.sql.shuffle.partitions", "2").config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config(
            "spark.sql.warehouse.dir", cls.temporary.name + "/warehouse"
        )
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")

    def test_pinned_input_and_real_delta_publication(self):
        kind = "example:wks:master-data--Asset:1.0.0"
        source_path = str(Path(self.temporary.name) / "source")
        self.spark.createDataFrame([
            ("test:master-data--Asset:a", "1", kind,
             '{"Name":"first","Links":["test:master-data--Asset:a:",null,"test:master-data--Asset:a:1"],'
             '"Entries":[{"Pointer/name":"test:master-data--Asset:a:"},null]}',
             True, {"b": "2", "a": "1"}),
        ], "id string, version string, kind string, data string, isActive boolean, extra map<string,string>").write.format("delta").save(source_path)
        pinned, snapshot = read_pinned_source(self.spark, source_path)
        self.spark.createDataFrame([
            ("test:master-data--Asset:a", "2", kind, '{"Name":"later"}', False, {"a": "2"}),
        ], "id string, version string, kind string, data string, isActive boolean, extra map<string,string>").write.format("delta").mode("append").save(source_path)
        self.assertEqual(snapshot["source_delta_version"], 0)
        self.assertTrue(snapshot["source_path"].startswith("file:"))
        self.assertEqual(pinned.count(), 1)
        schemas = {kind: {"x-osdu-schema-source": kind, "type": "object", "properties": {
            "data": {"type": "object", "properties": {
                "Name": {"type": "string"},
                "Links": {"type": "array", "items": {
                    "type": "string", "x-osdu-relationship": [{"GroupType": "master-data", "EntityType": "Asset"}],
                }},
                "Entries": {"type": "array", "items": {"type": "object", "properties": {
                    "Pointer/name": {"type": "string", "x-osdu-relationship": [
                        {"GroupType": "master-data", "EntityType": "Asset"}
                    ]},
                }}},
            }}
        }}}
        result = build_silver(pinned, schemas, "real-delta")
        try:
            event = publish_silver(
                result, snapshot, self.temporary.name + "/tables", self.temporary.name + "/journal",
            )
            self.assertEqual(event["status"], "succeeded")
            marker = Path(self.temporary.name) / "journal/real-delta/succeeded.json"
            self.assertEqual(json.loads(marker.read_text())["output_versions"], event["output_versions"])
            root = next(name for name in event["output_paths"] if name.endswith("__v1_0_0"))
            rows = self.spark.read.format("delta").option(
                "versionAsOf", event["output_versions"][root]
            ).load(event["output_paths"][root]).collect()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["osdu__data__Name"], "first")
            self.assertEqual(rows[0]["version"], "1")
            self.assertTrue(rows[0]["_silver_is_latest"])
            self.assertEqual(rows[0]["extra"], {"a": "1", "b": "2"})
            self.assertNotIn("osdu__data__Links", rows[0].asDict())
            self.assertNotIn("osdu__data__Entries", rows[0].asDict())
            children = next(name for name in event["output_paths"] if name.endswith("___links"))
            self.assertEqual(self.spark.read.format("delta").load(event["output_paths"][children]).count(), 3)
            entries = next(name for name in event["output_paths"] if name.endswith("___entries"))
            entry_rows = self.spark.read.format("delta").load(event["output_paths"][entries]).orderBy("ordinal").collect()
            self.assertEqual(entry_rows[0]["value__Pointer_name"], "test:master-data--Asset:a:")
            self.assertTrue(entry_rows[1]["_silver_element_is_null"])
            with self.assertRaises(Py4JJavaError):
                publish_silver(
                    result, snapshot, self.temporary.name + "/tables", self.temporary.name + "/journal",
                )
        finally:
            release_silver(result)


if __name__ == "__main__":
    unittest.main()
