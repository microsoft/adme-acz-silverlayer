"""Pure, synthetic tests of the declared Silver representation contract."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from adme_acz_silverlayer.schema_contract import (
    compile_schema, load_schema_directory, parse_json, parse_record_reference, project_record,
)
from tno_bronze import bronze_row


KIND = "example:wks:master-data--Asset:1.0.0"
TARGETS = [{"GroupType": "master-data", "EntityType": "Asset"}]


def schema_with(properties):
    return {
        "x-osdu-schema-source": KIND, "type": "object",
        "properties": {"data": {"type": "object", "properties": properties}},
    }


class SchemaContractTests(unittest.TestCase):
    def test_local_refs_and_compositions_preserve_all_properties(self):
        schema = schema_with({})
        schema["definitions"] = {"base": {
            "type": "object", "properties": {"Name": {"type": "string"}}
        }}
        schema["properties"]["data"] = {
            "type": "object", "allOf": [
                {"$ref": "#/definitions/base"},
                {"type": "object", "properties": {"Count": {"type": "integer"}}},
            ],
        }
        plan = compile_schema(schema, KIND)
        value, references = project_record({"data": '{"Name":"asset","Count":2,"Unknown":true}'}, plan)
        self.assertEqual(json.loads(value), {"data": {"Name": "asset", "Count": 2}})
        self.assertEqual(references, [])

    def test_compatible_object_alternatives_merge_fields(self):
        schema = schema_with({"Context": {"oneOf": [
            {"type": "object", "properties": {"A": {"type": "string"}}},
            {"type": "object", "properties": {"B": {"type": "integer"}}},
        ]}})
        value, _ = project_record({"data": '{"Context":{"B":3}}'}, compile_schema(schema, KIND))
        self.assertEqual(json.loads(value)["data"]["Context"], {"A": None, "B": 3})

    def test_mixed_alternatives_preserve_json_without_coercion(self):
        schema = schema_with({"Value": {"type": ["string", "number", "null"]}})
        plan = compile_schema(schema, KIND)
        for value in ("12", 12, None):
            with self.subTest(value=value):
                projected, _ = project_record({"data": json.dumps({"Value": value})}, plan)
                result = json.loads(projected)["data"]["Value"]
                self.assertEqual(json.loads(result) if result is not None else None, value)

    def test_mixed_coordinate_dimensions_remain_json(self):
        schema = schema_with({"Coordinates": {"oneOf": [
            {"type": "array", "items": {"type": "number"}},
            {"type": "array", "items": {"type": "array", "items": {"type": "number"}}},
            {"type": "array", "items": {"type": "array", "items": {
                "type": "array", "items": {"type": "number"}
            }}},
        ]}})
        plan = compile_schema(schema, KIND)
        for coordinates in ([10, 20], [[10, 20], [11, 21]], [[[10, 20], [11, 21], [10, 20]]]):
            projected, references = project_record({"data": json.dumps({"Coordinates": coordinates})}, plan)
            values = json.loads(projected)["data"]["Coordinates"]
            decoded = [json.loads(value) for value in values]
            self.assertEqual(decoded, coordinates)
            self.assertEqual(references, [])

    def test_nested_mixed_alternatives_collect_links_once(self):
        reference = {"type": "string", "x-osdu-relationship": TARGETS}
        schema = schema_with({"Context": {"anyOf": [
            {"oneOf": [{"type": "string"}, {"type": "object", "properties": {"Link": reference}}]},
            {"type": "array", "items": reference},
        ]}})
        plan = compile_schema(schema, KIND)
        target = "test:master-data--Asset:a:"
        for context, pointer in (({"Link": target}, "/data/Context/Link"), ([target], "/data/Context/0")):
            projected, references = project_record({"data": json.dumps({"Context": context})}, plan)
            self.assertEqual(json.loads(json.loads(projected)["data"]["Context"]), context)
            self.assertEqual(len(references), 1)
            self.assertEqual(references[0][0], pointer)

    def test_relationship_alternatives_do_not_duplicate_occurrences(self):
        reference = {"type": "string", "x-osdu-relationship": TARGETS}
        plan = compile_schema(schema_with({"Value": {"oneOf": [reference, {"type": "number"}]}}), KIND)
        self.assertEqual(len(project_record({"data": '{"Value":"test:master-data--Asset:a:"}'}, plan)[1]), 1)
        self.assertEqual(project_record({"data": '{"Value":12}'}, plan)[1], [])

    def test_opaque_map_relationships_fail_instead_of_disappearing(self):
        schema = schema_with({"Pointers": {"type": "object", "additionalProperties": {
            "type": "string", "x-osdu-relationship": TARGETS,
        }}})
        with self.assertRaisesRegex(ValueError, "opaque additionalProperties"):
            compile_schema(schema, KIND)

    def test_array_references_preserve_duplicates_nulls_and_ancestry(self):
        schema = schema_with({"Groups": {"type": "array", "items": {
            "type": "object", "properties": {"Links": {
                "type": "array", "items": {"type": "string", "x-osdu-relationship": TARGETS}
            }},
        }}})
        value = "test:master-data--Asset:a:"
        _, references = project_record(
            {"data": json.dumps({"Groups": [{"Links": [value, value, None]}, {"Links": [value]}]})},
            compile_schema(schema, KIND),
        )
        self.assertEqual([row[1] for row in references], [[0, 0], [0, 1], [0, 2], [1, 0]])
        self.assertEqual([row[2] for row in references], [value, value, None, value])
        self.assertEqual([row[0] for row in references], [
            "/data/Groups/0/Links/0", "/data/Groups/0/Links/1", "/data/Groups/0/Links/2", "/data/Groups/1/Links/0",
        ])

    def test_unannotated_id_like_fields_are_not_relationships(self):
        plan = compile_schema(schema_with({"AssetID": {"type": "string"}}), KIND)
        _, references = project_record({"data": '{"AssetID":"test:master-data--Asset:a:"}'}, plan)
        self.assertEqual(references, [])

    def test_array_annotation_is_applied_to_occurrences(self):
        plan = compile_schema(schema_with({"Links": {
            "type": "array", "items": {"type": "string"}, "x-osdu-relationship": TARGETS,
        }}), KIND)
        _, references = project_record({"data": '{"Links":["test:master-data--Asset:a:"]}'}, plan)
        self.assertEqual(references[0][3:5], ("test:master-data--Asset:a", None))

    def test_wrapper_payload_and_metadata_are_projected(self):
        schema = schema_with({"Name": {"type": "string"}})
        schema["properties"]["meta"] = {"type": "array", "items": {
            "type": "object", "properties": {"Pointer": {
                "type": "string", "x-osdu-relationship": TARGETS,
            }},
        }}
        wrapper = {"id": "test:master-data--Asset:a", "kind": KIND,
                   "data": {"Name": "wrapped"}, "meta": [{"Pointer": "test:master-data--Asset:b:"}]}
        value, references = project_record({"data": json.dumps(wrapper), "meta": None}, compile_schema(schema, KIND))
        self.assertEqual(json.loads(value)["data"]["Name"], "wrapped")
        self.assertEqual(references[0][0], "/meta/0/Pointer")

    def test_acz_fixture_without_embedded_identity_preserves_fields_and_references(self):
        schema = schema_with({
            "Name": {"type": "string"},
            "Parent": {"type": "string", "x-osdu-relationship": TARGETS},
        })
        record = {"id": "osdu:master-data--Asset:a", "kind": KIND,
                  "data": {"Name": "Wrapped asset", "Parent": "osdu:master-data--Asset:b:"}}
        source = bronze_row(record, "test")
        value, references = project_record(
            {"data": source[0], "id": source[2], "version": source[3], "kind": source[4]},
            compile_schema(schema, KIND),
        )
        self.assertEqual(json.loads(value)["data"],
                         {"Name": "Wrapped asset", "Parent": "test:master-data--Asset:b:"})
        self.assertEqual(references, [
            ("/data/Parent", [], "test:master-data--Asset:b:", "test:master-data--Asset:b", None, "pending"),
        ])

    def test_metadata_only_wrappers_preserve_outer_metadata_precedence(self):
        schema = schema_with({"Name": {"type": "string"}})
        schema["properties"]["modifyUser"] = {"type": "string"}
        plan = compile_schema(schema, KIND)
        for marker in ("meta", "createUser", "createTime", "modifyUser", "modifyTime"):
            with self.subTest(marker=marker):
                value, _ = project_record(
                    {"data": json.dumps({"data": {"Name": "wrapped"}, marker: None})}, plan,
                )
                self.assertEqual(json.loads(value)["data"]["Name"], "wrapped")
        wrapper = {"data": {"Name": "wrapped"}, "modifyUser": "wrapper-user"}
        for outer, expected in ((None, "wrapper-user"), ("row-user", "row-user")):
            value, _ = project_record({"data": json.dumps(wrapper), "modifyUser": outer}, plan)
            self.assertEqual(json.loads(value)["modifyUser"], expected)

    def test_metadata_wrappers_validate_optional_embedded_identities(self):
        plan = compile_schema(schema_with({"Name": {"type": "string"}}), KIND)
        fields = {"id": "test:master-data--Asset:a", "kind": KIND, "version": "2"}
        for name, invalid in (("id", "test:master-data--Asset:b"), ("kind", "other:wks:Asset:1.0.0"),
                              ("id", None), ("kind", 7), ("version", "3")):
            with self.subTest(name=name, invalid=invalid), self.assertRaisesRegex(ValueError, f"wrapper {name}"):
                project_record({**fields, "data": json.dumps({
                    "data": {"Name": "wrapped"}, "meta": None, name: invalid,
                })}, plan)
        for identity in ({"id": fields["id"]}, {"kind": KIND}, {"version": "0002"}):
            value, _ = project_record({**fields, "data": json.dumps({
                "data": {"Name": "wrapped"}, "meta": None, **identity,
            })}, plan)
            self.assertEqual(json.loads(value)["data"]["Name"], "wrapped")

    def test_declared_entity_data_property_is_not_silently_unwrapped(self):
        plan = compile_schema(schema_with({
            "data": {"type": "object", "properties": {"Name": {"type": "string"}}},
        }), KIND)
        value, _ = project_record({"data": '{"data":{"Name":"entity"}}'}, plan)
        self.assertEqual(json.loads(value)["data"]["data"]["Name"], "entity")
        for marker in ({"meta": None}, {"id": "test:master-data--Asset:a", "kind": KIND}):
            with self.subTest(marker=marker), self.assertRaisesRegex(ValueError, "Ambiguous payload"):
                project_record({"data": json.dumps({"data": {"Name": "entity"}, **marker})}, plan)

    def test_inherited_targets_and_patterns_are_narrowed(self):
        schema = schema_with({"Link": {"allOf": [
            {"type": "string", "x-osdu-relationship": [], "pattern": r"^test:.*:[0-9]*$"},
            {"type": "string", "x-osdu-relationship": [
                *TARGETS, {"GroupType": "master-data", "EntityType": "Other"}
            ]},
            {"type": "string", "x-osdu-relationship": TARGETS, "pattern": r"^test:master-data--Asset:a:[0-9]*$"},
        ]}})
        plan = compile_schema(schema, KIND)
        for value, status in (("test:master-data--Asset:a:", "pending"),
                              ("test:master-data--Other:a:", "invalid_reference"),
                              ("test:master-data--Asset:b:", "invalid_reference")):
            _, references = project_record({"data": json.dumps({"Link": value})}, plan)
            self.assertEqual(references[0][-1], status)

    def test_unrestricted_relationship_alternative_does_not_inherit_a_pattern(self):
        reference = {"type": "string", "x-osdu-relationship": TARGETS}
        restricted = {**reference, "pattern": r"^test:master-data--Asset:a:[0-9]*$"}
        for operator in ("anyOf", "oneOf"):
            for branches in ([restricted, reference], [reference, restricted]):
                with self.subTest(operator=operator, branches=branches):
                    plan = compile_schema(schema_with({"Link": {operator: branches}}), KIND)
                    _, references = project_record({"data": '{"Link":"test:master-data--Asset:b:1"}'}, plan)
                    self.assertEqual(references, [
                        ("/data/Link", [], "test:master-data--Asset:b:1", "test:master-data--Asset:b", "1", "pending"),
                    ])
                    _, invalid = project_record({"data": '{"Link":"test:master-data--Other:b:1"}'}, plan)
                    self.assertEqual(invalid[0][-1], "invalid_reference")

    def test_restricted_relationship_alternatives_keep_pattern_union(self):
        reference = {"type": "string", "x-osdu-relationship": TARGETS}
        plan = compile_schema(schema_with({"Link": {"anyOf": [
            {**reference, "pattern": r"^test:master-data--Asset:a:[0-9]*$"},
            {**reference, "pattern": r"^test:master-data--Asset:b:[0-9]*$"},
        ]}}), KIND)
        for asset, status in (("a", "pending"), ("b", "pending"), ("c", "invalid_reference")):
            with self.subTest(asset=asset):
                _, references = project_record(
                    {"data": json.dumps({"Link": f"test:master-data--Asset:{asset}:1"})}, plan,
                )
                self.assertEqual(references[0][-1], status)

    def test_nonrelationship_alternative_does_not_relax_reference_pattern(self):
        plan = compile_schema(schema_with({"Link": {"anyOf": [
            {"type": "string", "x-osdu-relationship": TARGETS,
             "pattern": r"^test:master-data--Asset:a:[0-9]*$"},
            {"type": "number"},
        ]}}), KIND)
        _, references = project_record({"data": '{"Link":"test:master-data--Asset:b:1"}'}, plan)
        self.assertEqual(references[0][-1], "invalid_reference")
        self.assertEqual(project_record({"data": '{"Link":12}'}, plan)[1], [])

    def test_node_annotations_cannot_override_inherited_relationship_constraints(self):
        schema = schema_with({"Link": {
            "allOf": [{"type": "string", "x-osdu-relationship": TARGETS,
                       "pattern": r"^test:master-data--Asset:a:[0-9]*$"}],
            "x-osdu-relationship": [],
            "pattern": r"^test:.*:[0-9]*$",
        }})
        plan = compile_schema(schema, KIND)
        for value in ("test:master-data--Other:a:", "test:master-data--Asset:b:"):
            with self.subTest(value=value):
                _, references = project_record({"data": json.dumps({"Link": value})}, plan)
                self.assertEqual(references[0][-1], "invalid_reference")
        schema["properties"]["data"]["properties"]["Link"]["x-osdu-relationship"] = [
            {"GroupType": "master-data", "EntityType": "Other"}
        ]
        with self.assertRaisesRegex(ValueError, "Contradictory inherited"):
            compile_schema(schema, KIND)

    def test_array_annotation_intersects_item_relationship_constraints(self):
        schema = schema_with({"Links": {
            "type": "array", "x-osdu-relationship": [],
            "items": {"type": "string", "x-osdu-relationship": TARGETS},
        }})
        plan = compile_schema(schema, KIND)
        _, references = project_record({"data": '{"Links":["test:master-data--Other:a:"]}'}, plan)
        self.assertEqual(references[0][-1], "invalid_reference")

    def test_escaped_property_paths_are_unambiguous(self):
        plan = compile_schema(schema_with({
            "a.b/c~": {"type": "string", "x-osdu-relationship": TARGETS}
        }), KIND)
        _, references = project_record({"data": json.dumps({"a.b/c~": "test:master-data--Asset:a:"})}, plan)
        self.assertEqual(references[0][0], "/data/a.b~1c~0")

    def test_wrapper_identity_and_version_mismatches_fail(self):
        plan = compile_schema(schema_with({"Name": {"type": "string"}}), KIND)
        wrapper = {"id": "test:master-data--Asset:a", "kind": KIND, "version": "2", "data": {"Name": "wrapped"}}
        with self.assertRaisesRegex(ValueError, "wrapper id"):
            project_record({"id": "test:master-data--Asset:b", "data": json.dumps(wrapper)}, plan)
        with self.assertRaisesRegex(ValueError, "wrapper version"):
            project_record({"version": "1", "data": json.dumps(wrapper)}, plan)

    def test_schema_directory_uses_exact_identity_and_safe_filenames(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / (KIND.replace(":", "_") + ".json")
            schema = schema_with({"Name": {"type": "string"}})
            path.write_text(json.dumps(schema))
            self.assertEqual(load_schema_directory(directory, [KIND]), {KIND: schema})
            with self.assertRaisesRegex(ValueError, "safely"):
                load_schema_directory(directory, ["../escape:wks:master-data--Asset:1.0.0"])
            schema["x-osdu-schema-source"] = "other:wks:master-data--Asset:1.0.0"
            path.write_text(json.dumps(schema))
            with self.assertRaisesRegex(ValueError, "identity"):
                load_schema_directory(directory, [KIND])

    def test_reference_grammar_includes_colons_in_entity_ids(self):
        self.assertEqual(parse_record_reference(
            "test:reference-data--CoordinateReferenceSystem:Projected:EPSG::32615:00012",
            [{"GroupType": "reference-data"}], None),
            ("test:reference-data--CoordinateReferenceSystem:Projected:EPSG::32615", "12", "pending"))

    def test_explicit_version_is_exact_decimal_and_not_float(self):
        self.assertEqual(parse_record_reference("test:master-data--Asset:a:0000", TARGETS, None)[1], "0")
        self.assertEqual(parse_record_reference("test:master-data--Asset:a:9999999999999999999999", TARGETS, None)[1],
                         "9999999999999999999999")
        for value in ("test:master-data--Asset:a:1.0", "test:master-data--Asset:a",
                      "test:master-data--Other:a:", "test:master-data--Asset:a:-1"):
            with self.subTest(value=value):
                self.assertEqual(parse_record_reference(value, TARGETS, None)[2], "invalid_reference")

    def test_declared_pattern_is_authoritative(self):
        pattern = r"^test:master-data--Asset:[a-z]+:[0-9]*$"
        self.assertEqual(parse_record_reference("test:master-data--Asset:a1:", TARGETS, pattern)[2], "invalid_reference")
        self.assertEqual(parse_record_reference("test:master-data--Asset:abc:", TARGETS, pattern)[2], "pending")

    def test_missing_optional_properties_do_not_emit_fake_relationships(self):
        plan = compile_schema(schema_with({"Link": {"type": "string", "x-osdu-relationship": TARGETS}}), KIND)
        self.assertEqual(project_record({"data": "{}"}, plan)[1], [])

    def test_schema_identity_mismatch_and_unsupported_references_fail(self):
        schema = schema_with({"Bad": {"$ref": "https://schemas.example/base"}})
        with self.assertRaisesRegex(ValueError, "identity"):
            compile_schema(schema, "other:wks:master-data--Asset:1.0.0")
        with self.assertRaisesRegex(ValueError, "local"):
            compile_schema(schema, KIND)
        schema["definitions"] = {"cycle": {"$ref": "#/definitions/cycle"}}
        schema["properties"]["data"]["properties"]["Bad"] = {"$ref": "#/definitions/cycle"}
        with self.assertRaisesRegex(ValueError, "Recursive"):
            compile_schema(schema, KIND)

    def test_tuple_array_conflicting_types_and_case_collisions_fail(self):
        nodes = (
            {"type": "array", "items": [{"type": "string"}]},
            {"allOf": [{"type": "string"}, {"type": "integer"}]},
            {"type": "object", "properties": {"Name": {"type": "string"}, "name": {"type": "string"}}},
        )
        for node in nodes:
            with self.subTest(node=node), self.assertRaises(ValueError):
                compile_schema(schema_with({"Bad": node}), KIND)

    def test_malformed_json_and_invalid_declared_values_fail(self):
        for payload in ('{"a":1,"a":2}', '{"a":NaN}', '{'):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                parse_json(payload)
        plan = compile_schema(schema_with({"Count": {"type": "integer"}}), KIND)
        for value in (True, 2**63, "2"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "declared type"):
                project_record({"data": json.dumps({"Count": value})}, plan)


if __name__ == "__main__":
    unittest.main()
