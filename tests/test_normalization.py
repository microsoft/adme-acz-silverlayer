"""Synthetic Spark transformations; no ADME, Fabric, or Delta writes."""

import ast
import importlib.util
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

try:
    from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
except ModuleNotFoundError as error:
    if error.name != "pyspark":
        raise
    T = None

if T is not None:
    from adme_acz_silverlayer import normalization, spark_schema


def java_is_available():
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin/java") if java_home else shutil.which("java")
    if not java:
        return False
    result = subprocess.run(
        [java, "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
    )
    return result.returncode == 0


def notebook_array_builders():
    notebook = json.loads((ROOT / "ADME ACZ Silver Layer.ipynb").read_text(encoding="utf-8"))
    names = {"_merge_key_column_names", "_build_child_primitive", "_build_child_struct", "_explode_typed_array"}
    nodes = []
    for cell in notebook["cells"]:
        if cell["cell_type"] != "code":
            continue
        nodes.extend(
            node
            for node in ast.parse("".join(cell["source"])).body
            if isinstance(node, ast.FunctionDef) and node.name in names
        )
    namespace = {
        "DataFrame": DataFrame, "F": F, "T": T,
        "logger": logging.getLogger("array_builder_tests"),
        "merge_key_columns": ["id", "version"],
        "explode_array": normalization.explode_array,
        "make_delta_column_alias": normalization.make_delta_column_alias,
        "_quoted_top_level_col": normalization._quoted_top_level_col,
        "_nested_field_col": normalization._nested_field_col,
        "_flatten_all_struct_columns": normalization.flatten_structs,
        "_dedupe_case_insensitive_struct_type": spark_schema.dedupe_struct_type,
        # JSON inference is a separate boundary; these fixtures use declared typed values.
        "_flatten_inferred_json_string_columns": lambda frame: frame,
        "_infer_json_schema": lambda *args, **kwargs: None,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<notebook-array-builders>", "exec"), namespace)
    return namespace


@unittest.skipIf(T is None, "Install the optional [spark] dependency for normalization helpers.")
class NormalizationAliasTests(unittest.TestCase):
    def test_aliases_are_safe_deterministic_and_unique(self):
        self.assertEqual(normalization.sanitize_delta_column_name("123 name"), "field_123_name")
        self.assertEqual(
            normalization.sanitize_delta_column_name("data.(COMPANY: comment)"),
            "data__COMPANY_comment",
        )
        used = {"id", "ordinal"}
        first = normalization.make_delta_column_alias("id", used)
        second = normalization.make_delta_column_alias("id", used)
        self.assertRegex(first, r"^id_[0-9a-f]{8}$")
        self.assertNotEqual(first, second)
        self.assertEqual(first, normalization.make_delta_column_alias("id", {"id", "ordinal"}))

    def test_alias_collisions_are_case_insensitive(self):
        used = {"id", "version", "ordinal", "Name"}
        for raw in ("ID", "Version", "Ordinal", "name", "NAME"):
            with self.subTest(raw=raw):
                reserved = {name.casefold() for name in used}
                alias = normalization.make_delta_column_alias(raw, used)
                self.assertNotIn(alias.casefold(), reserved)
                self.assertIn(alias, used)

    def test_alias_hash_and_numeric_suffix_collisions_are_case_insensitive(self):
        hashed = normalization.make_delta_column_alias("ID", {"id"})
        used = {"id", hashed.lower(), "id_2"}
        self.assertEqual(normalization.make_delta_column_alias("ID", used), "ID_3")


@unittest.skipIf(T is None, "Install the optional [spark] dependency to run real Spark tests.")
class SparkNormalizationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not java_is_available():
            raise unittest.SkipTest("A working Java runtime is required for local Spark tests.")
        cls.temporary = tempfile.TemporaryDirectory(prefix="adme-normalization-tests-")
        cls.python_environment = mock.patch.dict(os.environ, {
            "PYSPARK_PYTHON": sys.executable, "SPARK_LOCAL_IP": "127.0.0.1",
        })
        cls.python_environment.start()
        try:
            builder = (
                SparkSession.builder.master("local[2]")
                .appName("adme-normalization-tests")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.warehouse.dir", cls.temporary.name)
            )
            if importlib.util.find_spec("delta") is not None:
                from delta import configure_spark_with_delta_pip

                builder = builder.config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                ).config(
                    "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                )
                # The first Spark context owns the JVM classpath for the test process.
                builder = configure_spark_with_delta_pip(builder)
            cls.spark = builder.getOrCreate()
        except Exception:
            cls.temporary.cleanup()
            cls.python_environment.stop()
            raise
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.stop()
        finally:
            cls.temporary.cleanup()
            cls.python_environment.stop()

    def test_record_key_preserves_exact_strings_and_rejects_invalid_identity(self):
        identities = [
            ('test:Well:a,"\\雪', "0001"),
            ("test:Well:a", "90071992547409931234567890"),
            ("test:master-data--Well:sample", "0008"),
            ("test:Well:multi:segment", "0"),
        ]
        frame = self.spark.createDataFrame(identities, "id string, version string")
        keys = frame.select(normalization.record_key(F.col("id"), F.col("version")).alias("key")).collect()
        self.assertEqual([row.key for row in keys], [
            f"{record_id}:{version}" for record_id, version in identities
        ])
        for identity in [(None, "1"), ("id", None), ("", "1"), ("id", ""),
                         ("id", "1:2"), ("id", "-1"), ("id", "1.0"), ("id", " 1"),
                         ("id", "1\n"), ("id", "١"), ("id", "latest")]:
            with self.subTest(identity=identity), self.assertRaisesRegex(Exception, "Record identity requires"):
                self.spark.createDataFrame([identity], frame.schema).select(
                    normalization.record_key(F.col("id"), F.col("version")),
                ).collect()

    def test_child_builders_keep_parent_record_key_with_id_only_merge(self):
        helpers = notebook_array_builders()
        helpers["merge_key_columns"] = ["id"]
        frame = self.spark.createDataFrame(
            [("test:Well:a", "0001", '["a","b"]')], "id string, version string, items string",
        ).withColumn("record_key", normalization.record_key(F.col("id"), F.col("version")))
        child = helpers["_build_child_primitive"](frame, "items")
        self.assertEqual(child.columns, ["id", "record_key", "ordinal", "value"])
        self.assertEqual({row.record_key for row in child.collect()}, {'test:Well:a:0001'})

    def test_flatten_structs_preserves_versions_duplicates_and_array_values(self):
        schema = T.StructType([
            T.StructField("id", T.StringType()),
            T.StructField("version", T.StringType()),
            T.StructField("data", T.StructType([
                T.StructField("name", T.StringType()),
                T.StructField("location", T.StructType([
                    T.StructField("x", T.DoubleType()),
                ])),
                T.StructField("values", T.ArrayType(T.StringType())),
            ])),
        ])
        frame = self.spark.createDataFrame([
            ("test:master-data--Well:sample", "9", ("Sample", (1.5,), ["a", "a", None])),
            ("test:master-data--Well:sample", "9007199254740993", (None, None, [])),
            ("test:master-data--Well:sample", "9", ("Sample", (1.5,), ["a", "a", None])),
        ], schema)
        result = normalization.flatten_structs(frame)
        self.assertEqual(result.columns, ["id", "version", "data__name", "data__location__x", "data__values"])
        rows = result.collect()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], rows[2])
        self.assertEqual(rows[0]["data__values"], ["a", "a", None])
        self.assertEqual(rows[1]["version"], "9007199254740993")
        self.assertIsNone(rows[1]["data__location__x"])
        self.assertEqual(rows[1]["data__values"], [])

    def test_literal_dots_and_backticks_in_struct_names(self):
        schema = T.StructType([
            T.StructField("source.part", T.StructType([
                T.StructField("value`name", T.StringType()),
            ])),
        ])
        result = normalization.flatten_structs(self.spark.createDataFrame([(("sample",),)], schema))
        self.assertEqual(result.columns, ["source__part__value_name"])
        self.assertEqual(result.first()[0], "sample")

    def test_primitive_array_occurrences_keep_keys_duplicates_and_null_elements(self):
        frame = self.spark.createDataFrame([
            ("same", "9", ["a", "a", None]),
            ("same", "10", ["b"]),
            ("empty", "1", []),
            ("null", "1", None),
        ], "id string, version string, values array<string>")
        result = normalization.explode_array(frame, "values", ["id", "version"], T.StringType())
        actual = sorted(tuple(row) for row in result.collect())
        self.assertEqual(actual, [
            ("same", "10", 0, "b"),
            ("same", "9", 0, "a"),
            ("same", "9", 1, "a"),
            ("same", "9", 2, None),
        ])

    def test_struct_arrays_keep_ordinals_and_do_not_overwrite_identity_fields(self):
        element = T.StructType([
            T.StructField("id", T.StringType()),
            T.StructField("ordinal", T.IntegerType()),
            T.StructField("payload", T.StructType([
                T.StructField("value", T.DoubleType()),
            ])),
        ])
        schema = T.StructType([
            T.StructField("id", T.StringType()),
            T.StructField("version", T.StringType()),
            T.StructField("items", T.ArrayType(element)),
        ])
        frame = self.spark.createDataFrame([
            ("owner", "2", [("element", 7, (1.5,)), None]),
        ], schema)
        result = normalization.flatten_structs(
            normalization.explode_array(frame, "items", ["id", "version"], element)
        )
        rows = result.orderBy("ordinal").collect()
        self.assertEqual([row["id"] for row in rows], ["owner", "owner"])
        self.assertEqual([row["ordinal"] for row in rows], [0, 1])
        self.assertEqual(rows[0]["payload__value"], 1.5)
        self.assertIsNone(rows[1]["payload__value"])
        element_id = next(name for name in result.columns if name.startswith("id_"))
        self.assertEqual(rows[0][element_id], "element")
        self.assertIsNone(rows[1][element_id])

    def test_json_primitive_builder_matches_typed_array_builder(self):
        functions = notebook_array_builders()
        source = self.spark.createDataFrame([
            ("same", "2", '["a", "a", null]'),
            ("empty", "2", "[]"),
            ("absent", "2", None),
        ], "id string, version string, values string")
        json_result = functions["_build_child_primitive"](source, "values")
        typed = source.withColumn("values", F.from_json("values", "array<string>"))
        typed_result = functions["_explode_typed_array"](typed, "values", T.StringType())
        self.assertEqual(json_result.schema, typed_result.schema)
        self.assertEqual(
            sorted(tuple(row) for row in json_result.collect()),
            sorted(tuple(row) for row in typed_result.collect()),
        )

    def test_struct_array_case_variants_preserve_parent_keys(self):
        functions = notebook_array_builders()
        element = T.StructType([
            T.StructField("ID", T.StringType()),
            T.StructField("Version", T.StringType()),
            T.StructField("Ordinal", T.StringType()),
        ])
        schema = T.StructType([
            T.StructField("id", T.StringType()),
            T.StructField("version", T.StringType()),
            T.StructField("items", T.ArrayType(element)),
        ])
        source = self.spark.createDataFrame([
            ("owner", "9", [("element", "element-version", "element-ordinal"), None]),
        ], schema)
        for builder in ("typed", "json"):
            with self.subTest(builder=builder):
                if builder == "typed":
                    child = functions["_explode_typed_array"](source, "items", element)
                else:
                    serialized = source.withColumn("items", F.to_json("items"))
                    child = functions["_build_child_struct"](serialized, "items", T.ArrayType(element))
                rows = child.orderBy("ordinal").collect()
                self.assertEqual(child.select("id").first()[0], "owner")
                self.assertEqual(len(child.columns), len({name.casefold() for name in child.columns}))
                self.assertEqual(rows[0]["version"], "9")
                values = [rows[0][name] for name in child.columns if name not in {"id", "version", "ordinal"}]
                self.assertEqual(values, ["element", "element-version", "element-ordinal"])
                self.assertTrue(all(rows[1][name] is None for name in child.columns
                                    if name not in {"id", "version", "ordinal"}))

    def test_json_struct_builder_matches_typed_array_builder(self):
        functions = notebook_array_builders()
        element = T.StructType([
            T.StructField("name", T.StringType()),
            T.StructField("location", T.StructType([T.StructField("x", T.DoubleType())])),
        ])
        array_type = T.ArrayType(element)
        source = self.spark.createDataFrame([
            ("owner", "9", '[{"name":"a","location":{"x":1.5}},null]'),
            ("owner", "10", "[]"),
        ], "id string, version string, values string")
        json_result = functions["_build_child_struct"](source, "values", array_type)
        typed = source.withColumn("values", F.from_json("values", array_type))
        typed_result = functions["_explode_typed_array"](typed, "values", element)
        self.assertEqual(json_result.schema, typed_result.schema)
        self.assertEqual(
            json_result.orderBy("id", "version", "ordinal").collect(),
            typed_result.orderBy("id", "version", "ordinal").collect(),
        )

    def test_empty_struct_arrays_retain_the_declared_child_schema(self):
        functions = notebook_array_builders()
        array_type = T.ArrayType(T.StructType([T.StructField("value", T.DoubleType())]))
        source = self.spark.createDataFrame([
            ("empty", "1", "[]"), ("null", "1", None),
        ], "id string, version string, values string")
        result = functions["_build_child_struct"](source, "values", array_type)
        self.assertEqual(result.columns, ["id", "version", "ordinal", "value"])
        self.assertEqual(result.schema["value"].dataType, T.DoubleType())
        self.assertEqual(result.count(), 0)


if __name__ == "__main__":
    unittest.main()
