import ast
import hashlib
import json
import logging
import os
import re
import sys
import unittest
from dataclasses import dataclass
from functools import reduce
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Any
from unittest import mock

try:
    from pyspark.sql import Column, DataFrame, SparkSession, functions as F, types as T
except ModuleNotFoundError as exc:
    if exc.name != "pyspark":
        raise
    SparkSession = None


NOTEBOOK = Path(__file__).resolve().parents[1] / "ADME ACZ Silver Layer.ipynb"
KIND = "osdu:wks:master-data--Well:1.0.0"
PARENT = "osdu_wks_well"
FEATURES = "data__SpatialLocation__AsIngestedCoordinates__features"
FEATURE_CHILD = f"{PARENT}___spatiallocation__asingestedcoordinates__features"
ARRAY_COLUMNS = (
    FEATURES,
    "data__FacilityNameAliases",
    "data__FacilitySpecifications",
)
CHILDREN = {
    FEATURE_CHILD,
    f"{PARENT}___facilitynamealiases",
    f"{PARENT}___facilityspecifications",
}


def load_decomposition():
    anchors = {
        "SchemaRegistry", "_infer_json_schema", "classify_columns",
        "decompose_kind", "reassemble_kind",
    }
    nodes = []
    found = set()
    for cell in json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"]:
        if cell["cell_type"] != "code":
            continue
        tree = ast.parse("".join(cell["source"]))
        matches = {getattr(node, "name", "") for node in tree.body} & anchors
        if matches:
            found.update(matches)
            nodes.extend(
                node for node in tree.body
                if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Assign, ast.AnnAssign))
            )
    if found != anchors:
        raise AssertionError(f"Missing notebook decomposition definitions: {anchors - found}")
    namespace = dict(
        Any=Any, Column=Column, DataFrame=DataFrame, SparkSession=SparkSession,
        T=T, F=F, dataclass=dataclass, hashlib=hashlib, json=json, re=re,
        reduce=reduce, perf_counter=perf_counter, logger=logging.getLogger(__name__),
        merge_key_columns=["id", "version"],
    )
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(NOTEBOOK), "exec"), namespace)
    return namespace


@unittest.skipIf(SparkSession is None, "PySpark is not installed")
class NotebookArrayNormalizationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        environment = mock.patch.dict(os.environ, {
            "PYSPARK_PYTHON": sys.executable,
            "SPARK_LOCAL_IP": "127.0.0.1",
        })
        environment.start()
        cls.addClassCleanup(environment.stop)
        temporary = TemporaryDirectory(prefix="well-array-normalization-")
        cls.addClassCleanup(temporary.cleanup)
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("well-array-normalization")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.warehouse.dir", temporary.name)
            .getOrCreate()
        )
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.runtime = load_decomposition()
        number_array = {"type": "array", "items": {"type": "number"}}
        feature = {
            "type": "object",
            "properties": {
                "geometry": {
                    "type": "object",
                    "properties": {
                        "coordinates": number_array,
                        "type": {"type": "string"},
                        "bbox": number_array,
                    },
                },
                "type": {"type": "string"},
                "properties": {"type": "object"},
                "bbox": number_array,
            },
        }
        object_array = {
            "type": "array",
            "items": {"type": "object", "properties": {"name": {"type": "string"}}},
        }
        schema = {
            "type": "object",
            "$defs": {"Feature": feature},
            "properties": {
                "id": {"type": "string"},
                "version": {"type": "string"},
                "kind": {"type": "string"},
                "data": {
                    "type": "object",
                    "properties": {
                        "FacilityNameAliases": object_array,
                        "FacilitySpecifications": object_array,
                        "ScalarSamples": number_array,
                        "SpatialLocation": {
                            "type": "object",
                            "properties": {
                                "AsIngestedCoordinates": {
                                    "type": "object",
                                    "properties": {
                                        "features": {
                                            "type": "array",
                                            "items": {"$ref": "#/$defs/Feature"},
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        }
        cls.registry = cls.runtime["SchemaRegistry"].from_dict({KIND: schema})

    def decompose(self, values, create_empty):
        rows = [
            (
                f"test:master-data--Well:sample-{index}", "0007", KIND,
                json.dumps({
                    "FacilityNameAliases": None,
                    "FacilitySpecifications": None,
                    "ScalarSamples": [1.0, 2.0],
                    "SpatialLocation": {"AsIngestedCoordinates": {"features": features}},
                }),
            )
            for index, features in enumerate(values)
        ]
        frame = self.spark.createDataFrame(rows, "id string, version string, kind string, data string")
        self.runtime["create_empty_child_tables"] = create_empty
        return self.runtime["decompose_kind"](
            frame, PARENT, registry=self.registry, osdu_kind=KIND, input_rows=len(rows),
        )

    def assert_parent(self, parent, row_count):
        self.assertEqual(set(ARRAY_COLUMNS) & set(parent.columns), set())
        columns = ["id", "version", "data__ScalarSamples"]
        if "record_key" in parent.columns:
            columns.append("record_key")
        rows = parent.select(*columns).collect()
        self.assertEqual(len(rows), row_count)
        self.assertEqual(len({(row.id, row.version) for row in rows}), row_count)
        for row in rows:
            self.assertEqual(row.version, "0007")
            self.assertEqual(json.loads(row.data__ScalarSamples), [1.0, 2.0])
            if "record_key" in columns:
                self.assertEqual(row.record_key, f"{row.id}:{row.version}")

    def test_empty_nested_arrays_have_stable_parent_and_optional_typed_children(self):
        for values in ([None, None], [[], []], [None, []], []):
            for create_empty in (True, False):
                with self.subTest(values=values, create_empty_child_tables=create_empty):
                    decomposed = self.decompose(values, create_empty)
                    self.assert_parent(decomposed.parent, len(values))
                    self.assertEqual(set(decomposed.children), CHILDREN if create_empty else set())
                    for _, child in decomposed.children.values():
                        self.assertEqual(child.count(), 0)
                        self.assertEqual(child.schema["ordinal"].dataType, T.IntegerType())
                        self.assertEqual(child.schema["version"].dataType, T.StringType())
                    if create_empty:
                        child = decomposed.children[FEATURE_CHILD][1]
                        self.assertEqual(child.schema["geometry__coordinates"].dataType, T.ArrayType(T.DoubleType()))
                        self.assertEqual(child.schema["properties"].dataType, T.MapType(T.StringType(), T.StringType()))
                    self.assert_parent(self.runtime["reassemble_kind"](decomposed), len(values))

    def test_populated_features_preserve_values_ordinals_and_parent_identity(self):
        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [4.0, 58.0], "bbox": [4.0, 58.0, 4.0, 58.0]},
            "properties": {"label": "sample"},
            "bbox": [4.0, 58.0, 4.0, 58.0],
        }
        for create_empty in (True, False):
            with self.subTest(create_empty_child_tables=create_empty):
                decomposed = self.decompose([None, [], [feature, None, feature]], create_empty)
                self.assert_parent(decomposed.parent, 3)
                self.assertEqual(
                    set(decomposed.children), CHILDREN if create_empty else {FEATURE_CHILD},
                )
                rows = decomposed.children[FEATURE_CHILD][1].orderBy("ordinal").collect()
                self.assertEqual([row.ordinal for row in rows], [0, 1, 2])
                self.assertEqual({(row.id, row.version) for row in rows}, {("test:master-data--Well:sample-2", "0007")})
                if "record_key" in decomposed.children[FEATURE_CHILD][1].columns:
                    self.assertEqual({row.record_key for row in rows}, {"test:master-data--Well:sample-2:0007"})
                for row in (rows[0], rows[2]):
                    self.assertEqual(row.geometry__coordinates, [4.0, 58.0])
                    self.assertEqual(row.properties, {"label": "sample"})
                    self.assertEqual(row.geometry__type, "Point")
                self.assertIsNone(rows[1].geometry__coordinates)
                self.assertIsNone(rows[1].properties)

    def test_long_child_names_preserve_distinct_sources_and_wide_columns(self):
        fields = ["measurements_" * 10 + suffix for suffix in ("first", "second")]
        schema = {
            "properties": {
                "id": {"type": "string"},
                "version": {"type": "string"},
                "data": {
                    "type": "object",
                    "properties": {
                        field: {
                            "type": "array",
                            "items": {"type": "object", "properties": {"label": {"type": "string"}}},
                        }
                        for field in fields
                    },
                },
            },
        }
        registry = self.runtime["SchemaRegistry"].from_dict({KIND: schema})
        payload = {field: [{"label": value}] for field, value in zip(fields, ("alpha", "beta"))}
        frame = self.spark.createDataFrame(
            [("test:master-data--Well:sample", "0007", json.dumps(payload))],
            "id string, version string, data string",
        )
        for parent in (PARENT, "p" * 128):
            with self.subTest(parent_length=len(parent)):
                decomposed = self.runtime["decompose_kind"](frame, parent, registry=registry, osdu_kind=KIND)
                self.assertEqual(len(decomposed.children), 2)
                for name, (source, child) in decomposed.children.items():
                    self.assertEqual(len(name), 128)
                    self.assertTrue(name.startswith(self.runtime["_child_table_prefix"](parent)))
                    self.assertIn(source, [f"data__{field}" for field in fields])
                    self.assertEqual(child.count(), 1)
                wide = self.runtime["reassemble_kind"](decomposed).first()
                for field, value in zip(fields, ("alpha", "beta")):
                    self.assertEqual(wide[f"{field}_0_label"], value)


if __name__ == "__main__":
    unittest.main()
