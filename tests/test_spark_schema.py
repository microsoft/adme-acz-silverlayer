"""Schema/type tests need PySpark, but do not start Spark or require Java."""

import copy
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

try:
    from pyspark.sql import types as T
except ModuleNotFoundError as error:
    if error.name != "pyspark":
        raise
    T = None

if T is not None:
    from adme_acz_silverlayer import spark_schema


@unittest.skipIf(T is None, "Install the optional [spark] dependency to test Spark schema conversion.")
class SparkSchemaTests(unittest.TestCase):
    def test_scalar_and_nullable_types(self):
        cases = [
            ("string", T.StringType()),
            ("integer", T.LongType()),
            ("number", T.DoubleType()),
            ("boolean", T.BooleanType()),
        ]
        for json_type, expected in cases:
            with self.subTest(json_type=json_type):
                self.assertEqual(spark_schema.json_schema_to_spark({"type": json_type}), expected)
                self.assertEqual(
                    spark_schema.json_schema_to_spark({"type": ["null", json_type]}), expected
                )

    def test_nested_objects_arrays_and_maps(self):
        schema = {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "value": {"type": "number"},
                            "attributes": {"type": "object"},
                        },
                    },
                },
            },
        }
        before = copy.deepcopy(schema)
        expected = T.StructType([
            T.StructField("items", T.ArrayType(T.StructType([
                T.StructField("value", T.DoubleType()),
                T.StructField("attributes", T.MapType(T.StringType(), T.StringType())),
            ]))),
        ])
        self.assertEqual(spark_schema.json_schema_to_spark(schema), expected)
        self.assertEqual(schema, before)

    def test_definition_references_and_allof(self):
        definitions = {
            "Identity": {
                "type": "object",
                "properties": {"id": {"type": "string"}},
            },
            "Measurement": {
                "type": "object",
                "properties": {"value": {"type": "number"}},
            },
        }
        schema = {"allOf": [
            {"$ref": "#/definitions/Identity"},
            {"$ref": "#/$defs/Measurement"},
        ]}
        self.assertEqual(
            spark_schema.json_schema_to_spark(schema, definitions),
            T.StructType([
                T.StructField("id", T.StringType()),
                T.StructField("value", T.DoubleType()),
            ]),
        )
        self.assertEqual(
            spark_schema._resolve_node(schema, definitions),
            {"type": "object", "properties": {
                "id": {"type": "string"}, "value": {"type": "number"},
            }},
        )

    def test_existing_alternative_and_unknown_reference_compatibility(self):
        for operator in ("oneOf", "anyOf"):
            with self.subTest(operator=operator):
                schema = {operator: [{"type": "null"}, {"type": "integer"}, {"type": "string"}]}
                self.assertEqual(spark_schema.json_schema_to_spark(schema), T.LongType())
        self.assertEqual(
            spark_schema.json_schema_to_spark({"$ref": "#/definitions/Unknown"}, {}),
            T.StringType(),
        )
        self.assertEqual(spark_schema.json_schema_to_spark({}), T.StringType())

    def test_parse_osdu_envelope_and_data_composition(self):
        schema = {
            "$defs": {
                "Envelope": {"properties": {"id": {"type": "string"}}},
                "Data": {"properties": {"Name": {"type": "string"}}},
            },
            "allOf": [{"$ref": "#/$defs/Envelope"}],
            "properties": {
                "version": {"type": "string"},
                "data": {"allOf": [
                    {"$ref": "#/$defs/Data"},
                    {"properties": {"Values": {"type": "array", "items": {"type": "number"}}}},
                ]},
            },
        }
        before = copy.deepcopy(schema)
        parsed = spark_schema.parse_osdu_schema(json.dumps(schema))
        self.assertEqual(set(parsed["envelope_fields"]), {"id", "version"})
        self.assertEqual(set(parsed["data_fields"]), {"Name", "Values"})
        self.assertEqual(parsed["definitions"], schema["$defs"])
        self.assertEqual(schema, before)
        self.assertEqual(spark_schema.parse_osdu_schema(schema), parsed)

    def test_case_insensitive_deduplication_preserves_metadata_and_nested_types(self):
        schema = T.StructType([
            T.StructField("Name", T.StringType(), False, {"description": "Synthetic field"}),
            T.StructField("name", T.StringType(), True),
            T.StructField("children", T.ArrayType(T.StructType([
                T.StructField("Value", T.NullType()),
                T.StructField("value", T.DoubleType()),
            ]), False)),
        ])
        expected = T.StructType([
            T.StructField("Name", T.StringType(), True, {"description": "Synthetic field"}),
            T.StructField("children", T.ArrayType(T.StructType([
                T.StructField("value", T.DoubleType()),
            ]), False)),
        ])
        self.assertEqual(spark_schema.dedupe_struct_type(schema), expected)

    def test_existing_type_merge_precedence(self):
        self.assertEqual(
            spark_schema._merge_schemas(T.StringType(), T.LongType()), T.StringType()
        )
        self.assertEqual(
            spark_schema._merge_schemas(T.NullType(), T.LongType()), T.LongType()
        )
        self.assertEqual(
            spark_schema._merge_schemas(T.ArrayType(T.StringType(), False), T.ArrayType(T.StringType())),
            T.ArrayType(T.StringType(), True),
        )


if __name__ == "__main__":
    unittest.main()
