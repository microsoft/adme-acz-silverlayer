"""Scalar relationship identities and synthetic Spark resolution coverage."""

import ast
import hashlib
import json
import os
import re
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from test_normalization import ROOT, T, java_is_available
from test_notebook_simplification import load_notebook

if T is not None:
    from pyspark.sql import SparkSession, Window, functions as F


def relationship_helpers():
    notebook = load_notebook()
    names = {
        "record_key",
        "_record_key_parts",
        "resolve_direct_relationships",
        "_relationship_table_prefix",
        "_is_relationship_bridge",
        "relationship_bridge_table_name",
        "relationship_bridge_tables_for_kind",
        "_relationship_field_columns",
        "_project_relationship_fields",
        "_bounded_relationship_name",
        "_validated_historical_outputs",
        "_child_table_prefix",
        "_child_table_suffix",
        "_sanitize_table_name_part",
        "child_table_name",
        "_is_array_child",
        "validate_table_names",
        "kind_to_table_name",
        "kind_to_versioned_table_name",
        "kind_parts",
        "kind_version",
    }
    nodes = [
        node for cell in notebook["cells"] if cell["cell_type"] == "code"
        for node in ast.parse("".join(cell["source"])).body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    if {node.name for node in nodes} != names:
        raise AssertionError("Missing notebook relationship helpers")
    module = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {
        "Column": object, "DataFrame": object, "SchemaRegistry": object, "re": re, "hashlib": hashlib,
        "_TABLE_NAME_PATTERN": re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$"), "_DATA_PREFIX": "data__",
    }
    if T is not None:
        namespace.update({
            "F": F, "T": T, "Window": Window,
            "_quoted_top_level_col": lambda name: F.col("`" + name.replace("`", "``") + "`"),
        })
    exec(compile(module, "<notebook-relationships>", "exec"), namespace)
    return namespace


class RelationshipFieldIdentityTests(unittest.TestCase):
    def test_notebook_does_not_use_a_global_relationship_marker(self):
        source = "\n".join("".join(cell["source"]) for cell in load_notebook()["cells"])
        self.assertNotIn("relationship__", source)
        self.assertNotIn("__r__", source)

    def test_reported_wks_bridge_names_fit_the_fabric_limit(self):
        helpers = relationship_helpers()
        bridge_name = helpers["relationship_bridge_table_name"]
        shared_fields = {
            "ResourceCurationStatus": "ResourceCurationStatus",
            "ResourceLifecycleStatus": "ResourceLifecycleStatus",
            "ResourceSecurityClassification": "ResourceSecurityClassification",
            "TechnicalAssuranceID": "TechnicalAssuranceType",
        }
        cases = {
            "reference-data--CoordinateReferenceSystem": shared_fields,
            "work-product-component--Document": {
                "ResourceSecurityClassification": "ResourceSecurityClassification",
            },
            "work-product-component--WellboreMarkerSet": shared_fields,
            "work-product-component--WellboreTrajectory": {
                **shared_fields, "CalculationMethodType": "CalculationMethodType",
            },
            "work-product-component--WellLog": {
                "ResourceSecurityClassification": "ResourceSecurityClassification",
            },
            "work-product--WorkProduct": {
                "ResourceSecurityClassification": "ResourceSecurityClassification",
            },
        }
        self.assertEqual(sum(len(fields) for fields in cases.values()), 16)
        for strategy in ("merge", "versioned_tables"):
            for prefix in ("", "fixture_"):
                generated = []
                for entity, fields in cases.items():
                    kind = f"osdu:wks:{entity}:1.0.0"
                    parent = f"{prefix}osdu_wks_{entity.split('--')[1].lower()}"
                    if strategy == "versioned_tables":
                        parent += "__v1_0_0"
                    for field, target in fields.items():
                        with self.subTest(strategy=strategy, prefix=prefix, entity=entity, field=field):
                            unbounded = (
                                f"{parent}__{field.lower()}"
                                f"__reference_data_{target.lower()}"
                            )
                            name = bridge_name(
                                kind, f"data__{field}", f"reference-data--{target}", prefix, strategy,
                            )
                            digest = hashlib.sha256(unbounded.encode()).hexdigest()[:16]
                            self.assertEqual(name, unbounded if len(unbounded) <= 128 else unbounded[:110] + "__" + digest)
                            self.assertLessEqual(len(name), 128)
                            self.assertTrue(helpers["_is_relationship_bridge"](name, parent))
                            self.assertNotIn("relationship__", name)
                            helpers["validate_table_names"]([name])
                            generated.append(name)
                self.assertEqual(len(set(generated)), 16)

    def test_bridge_name_length_boundaries_and_tail_disambiguation(self):
        helpers = relationship_helpers()
        name_for = helpers["relationship_bridge_table_name"]
        kind = "osdu:wks:master-data--Well:1.0.0"
        prefix = helpers["_relationship_table_prefix"]("osdu_wks_well")
        for length in (127, 128, 129):
            path = "x" * (length - len(prefix) - len("__target"))
            unbounded = prefix + path + "__target"
            name = name_for(kind, path, "target", version_strategy="merge")
            self.assertEqual(len(name), min(length, 128))
            if length <= 128:
                self.assertEqual(name, unbounded)
            else:
                self.assertNotEqual(
                    name, name_for(kind, path, "targeu", version_strategy="merge"),
                )
        helpers["validate_table_names"](["t" * 128])
        with self.assertRaisesRegex(ValueError, "128-character"):
            helpers["validate_table_names"](["t" * 129])
        with self.assertRaisesRegex(ValueError, "Invalid output table"):
            helpers["validate_table_names"](["invalid-name"])

    def test_bounded_source_namespace_preserves_historical_ownership(self):
        helpers = relationship_helpers()
        for strategy in ("merge", "versioned_tables"):
            kinds = [f"osdu:wks:work-product-component--{'x' * 90}{suffix}:1.0.0" for suffix in ("a", "b")]
            names = [
                helpers["relationship_bridge_table_name"](
                    kind, "data__ResourceCurationStatus", "reference-data--ResourceCurationStatus",
                    "fixture_", strategy,
                )
                for kind in kinds
            ]
            self.assertNotEqual(*names)
            for kind, name in zip(kinds, names):
                parent = "fixture_" + helpers[
                    "kind_to_table_name" if strategy == "merge" else "kind_to_versioned_table_name"
                ](kind)
                prefix = helpers["_relationship_table_prefix"](parent)
                self.assertLessEqual(len(prefix), 109)
                self.assertTrue(name.startswith(prefix))
                self.assertTrue(helpers["_is_relationship_bridge"](name, parent))
                self.assertEqual(len(name), 128)
                helpers["_manifest_child_tables_for_parent"] = lambda *args: [name]
                self.assertEqual(
                    helpers["_validated_historical_outputs"](
                        None, parent, "workspace", "lakehouse",
                    ), [name],
                )
            helpers["_manifest_child_tables_for_parent"] = lambda *args: names
            parent = "fixture_" + helpers[
                "kind_to_table_name" if strategy == "merge" else "kind_to_versioned_table_name"
            ](kinds[0])
            with self.assertRaisesRegex(RuntimeError, "ownership namespace"):
                helpers["_validated_historical_outputs"](
                    None, parent, "workspace", "lakehouse",
                )
        with self.assertRaisesRegex(ValueError, "128-character"):
            helpers["relationship_bridge_table_name"](
                kinds[0], "data__Field", "master-data--Well", "prefix_" * 20,
            )

    def test_bridges_remain_distinct_from_array_tables_at_parent_length_boundaries(self):
        helpers = relationship_helpers()
        for length in (106, 107, 108, 128):
            kind = f"osdu:wks:master-data--{'x' * (length - len('osdu_wks_'))}:1.0.0"
            parent = helpers["kind_to_table_name"](kind)
            self.assertEqual(len(parent), length)
            for field in ("Field", "---", "_Field", "Field__ID"):
                with self.subTest(parent_length=length, field=field):
                    name = helpers["relationship_bridge_table_name"](
                        kind, f"data__{field}", "master-data--Well", version_strategy="merge",
                    )
                    self.assertEqual(len(name), 128)
                    self.assertTrue(helpers["_is_relationship_bridge"](name, parent))
                    self.assertFalse(helpers["_is_relationship_bridge"](parent + "___items", parent))
                    self.assertFalse(helpers["_is_relationship_bridge"](parent, parent))
                    self.assertFalse(helpers["_is_relationship_bridge"](helpers["_relationship_table_prefix"](parent), parent))

    def test_bounded_array_names_preserve_ownership_and_remain_distinct_from_bridges(self):
        helpers = relationship_helpers()
        for length in (106, 107, 108, 124, 128):
            kind = f"osdu:wks:master-data--{'x' * (length - len('osdu_wks_'))}:1.0.0"
            parent = helpers["kind_to_table_name"](kind)
            arrays = [helpers["child_table_name"](parent, path) for path in ("data__x", "data__nested__" + "features" * 20)]
            bridge = helpers["relationship_bridge_table_name"](
                kind, "data__FacilityTypeID", "reference-data--FacilityType", version_strategy="merge",
            )
            with self.subTest(parent_length=length):
                for table in arrays:
                    helpers["validate_table_names"]([table])
                    self.assertTrue(helpers["_is_array_child"](table, parent))
                    self.assertFalse(helpers["_is_relationship_bridge"](table, parent))
                self.assertFalse(helpers["_is_array_child"](bridge, parent))
                self.assertFalse(helpers["_is_array_child"](helpers["_child_table_prefix"](parent), parent))
                allowed = [*arrays, bridge]
                helpers["_manifest_child_tables_for_parent"] = lambda *args: allowed
                self.assertEqual(helpers["_validated_historical_outputs"](None, parent, "", ""), allowed)
                other_parent = parent[:-1] + "y"
                foreign = helpers["child_table_name"](other_parent, "data__nested__" + "features" * 20)
                helpers["_manifest_child_tables_for_parent"] = lambda *args: [*allowed, foreign]
                with self.assertRaisesRegex(RuntimeError, "ownership namespace"):
                    helpers["_validated_historical_outputs"](None, parent, "", "")

    def test_paths_and_bridge_names_preserve_colliding_field_identities(self):
        helpers = relationship_helpers()
        fields = ("Primary-ID", "Primary_ID", "Primary__ID", "Primary.ID", "Primary`ID")
        relationships = [
            {"field": field, "targets": [{"GroupType": "master-data", "EntityType": "Well"}]}
            for field in fields
        ]
        registry = SimpleNamespace(direct_relationship_fields=lambda kind: relationships)
        columns = helpers["_relationship_field_columns"](relationships)
        self.assertEqual(columns, {field: f"data__{field}" for field in fields})
        plan = helpers["relationship_bridge_tables_for_kind"]
        kind = "osdu:wks:master-data--Wellbore:1.0.0"
        tables = plan(kind, registry, version_strategy="merge")
        self.assertEqual(len(tables), len(fields))
        self.assertEqual(len(set(tables)), len(fields))
        for field, table in zip(fields, tables):
            with self.subTest(field=field):
                subset = SimpleNamespace(direct_relationship_fields=lambda kind: [
                    relationship for relationship in relationships if relationship["field"] == field
                ])
                self.assertEqual(plan(kind, subset, version_strategy="merge"), [table])
                self.assertEqual(
                    plan("osdu:wks:master-data--Wellbore:2.0.0", subset, version_strategy="merge"),
                    [table],
                )
        relationships.reverse()
        self.assertEqual(plan(kind, registry, version_strategy="merge"), list(reversed(tables)))

    def test_remaining_bridge_name_collision_fails_closed(self):
        helpers = relationship_helpers()
        suffix = hashlib.sha256(b"data__Primary-ID").hexdigest()[:12]
        registry = SimpleNamespace(direct_relationship_fields=lambda kind: [
            {"field": field, "targets": [{"GroupType": "master-data", "EntityType": "Well"}]}
            for field in ("Primary-ID", f"Primary_ID_{suffix}")
        ])
        with self.assertRaisesRegex(ValueError, "Distinct scalar relationships"):
            helpers["relationship_bridge_tables_for_kind"](
                "osdu:wks:master-data--Wellbore:1.0.0", registry,
            )


@unittest.skipIf(T is None, "Install the optional [spark] dependency for relationship bridge integration tests.")
class RelationshipBridgeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not java_is_available():
            raise unittest.SkipTest("A working Java runtime is required for local Spark tests.")
        cls.temporary = tempfile.TemporaryDirectory(prefix="relationship-bridge-tests-")
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.environment = mock.patch.dict(os.environ, {
            "PYSPARK_PYTHON": sys.executable,
            "SPARK_LOCAL_IP": "127.0.0.1",
            "PYTHONPATH": str(ROOT / "src") + os.pathsep + os.environ.get("PYTHONPATH", ""),
        })
        cls.environment.start()
        cls.addClassCleanup(cls.environment.stop)
        cls.spark = SparkSession.builder.master("local[2]").appName("relationship-bridge-tests").config(
            "spark.ui.enabled", "false"
        ).config("spark.driver.host", "127.0.0.1").config(
            "spark.driver.bindAddress", "127.0.0.1"
        ).config("spark.sql.shuffle.partitions", "2").config(
            "spark.sql.warehouse.dir", cls.temporary.name
        ).getOrCreate()
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")

        namespace = relationship_helpers()
        cls.resolve_relationships = staticmethod(namespace["resolve_direct_relationships"])
        cls.bridge_table_name = staticmethod(namespace["relationship_bridge_table_name"])
        cls.project_relationships = staticmethod(namespace["_project_relationship_fields"])
        cls.plan_tables = staticmethod(namespace["relationship_bridge_tables_for_kind"])

    def test_record_key_decoder_preserves_final_colon_identity_and_rejects_invalid_keys(self):
        helpers = relationship_helpers()
        identities = [
            ("test:master-data--Well:sample", "0008"),
            ('test:Well:quote"\\雪\n:segment', "90071992547409931234567890"),
            ("test:Well:1", "2"),
            ("test:Well", "12"),
        ]
        frame = self.spark.createDataFrame(identities, "id string, version string").select(
            helpers["record_key"](F.col("id"), F.col("version")).alias("key"),
        )
        decoded = helpers["_record_key_parts"](F.col("key"))
        self.assertEqual([tuple(row) for row in frame.select(*decoded).collect()], identities)
        self.assertEqual(len({row.key for row in frame.collect()}), len(identities))
        for key in (None, "", ":1", "id:", "id:-1", "id:1\n", '["id","1"]'):
            with self.subTest(key=key), self.assertRaisesRegex(Exception, "Invalid record key"):
                self.spark.createDataFrame([(key,)], "key string").select(*decoded).collect()

    def test_lossless_projection_resolves_each_original_field(self):
        fields = ("Primary-ID", "Primary_ID", "Primary__ID", "Primary.ID", "Primary`ID")
        kind = "osdu:wks:master-data--Wellbore:1.0.0"
        references = {
            field: f"osdu:master-data--Well:target-{index}:"
            for index, field in enumerate(fields)
        }
        bronze = self.spark.createDataFrame([
            ("bore", "1", kind, json.dumps({"Primary ID": "decoy", **references})),
            ("empty", "1", kind, json.dumps({"Primary-ID": None})),
        ], "id string, version string, kind string, data string")
        targets = self.spark.createDataFrame([
            (reference[:-1], "1", "osdu:wks:master-data--Well:1.0.0", True)
            for reference in references.values()
        ], "id string, version string, kind string, isActive boolean")
        registry = SimpleNamespace(direct_relationship_fields=lambda kind: [
            {"field": field, "targets": [{"GroupType": "master-data", "EntityType": "Well"}]}
            for field in fields
        ])
        projected = self.project_relationships(bronze, registry, kind)
        _, bridge, tables = self.resolve_relationships(projected, targets, registry, kind)
        rows = bridge.collect()
        self.assertEqual(
            {(row.source_record_key, row._relationship_bridge_table, row.target_record_key.rsplit(":", 1)[0]) for row in rows},
            {('bore:1', self.bridge_table_name(kind, f"data__{field}", "master-data--Well"), reference[:-1]) for field, reference in references.items()},
        )
        self.assertEqual(len(rows), len(fields))
        self.assertEqual(tables, self.plan_tables(kind, registry))
        self.assertEqual(set(tables), {row._relationship_bridge_table for row in rows})

    def test_merge_bridge_names_omit_source_schema_version(self):
        self.assertEqual(
            self.bridge_table_name(
                "osdu:wks:master-data--Well:1.0.0",
                "data__ExistenceKind",
                "reference-data--ExistenceKind",
                version_strategy="merge",
            ),
            "osdu_wks_well__existencekind__reference_data_existencekind",
        )
        self.assertEqual(
            self.bridge_table_name(
                "osdu:wks:master-data--Well:1.0.0",
                "data__ExistenceKind",
                "reference-data--ExistenceKind",
            ),
            "osdu_wks_well__v1_0_0__existencekind__reference_data_existencekind",
        )
        self.assertEqual(
            self.bridge_table_name(
                "osdu:wks:master-data--Well:2.0.0",
                "data__ExistenceKind",
                "reference-data--ExistenceKind",
                version_strategy="merge",
            ),
            "osdu_wks_well__existencekind__reference_data_existencekind",
        )

    def test_scalar_relationship_projects_joinable_id_and_exact_or_latest_version(self):
        from pyspark.sql import Row

        well_kind = "osdu:wks:master-data--Well:1.0.0"
        well_kind_v2 = "osdu:wks:master-data--Well:2.0.0"
        wellbore_kind = "osdu:wks:master-data--Wellbore:1.0.0"
        well_id = "osdu:master-data--Well:well-a"
        other_id = "osdu:master-data--Well:well-b"
        source = self.spark.createDataFrame([
            (well_id, "0008", well_kind, True),
            (well_id, "9", well_kind_v2, False),
            (other_id, "2", well_kind, True),
            ("osdu:master-data--Wellbore:wellbore-a", "1", wellbore_kind, True),
        ], "id string, version string, kind string, isActive boolean")
        parent = self.spark.createDataFrame([
            ("osdu:master-data--Wellbore:wb-1", "1", wellbore_kind, well_id + ":"),
            ("osdu:master-data--Wellbore:wb-2", "1", wellbore_kind, well_id + ":0008"),
            ("osdu:master-data--Wellbore:wb-3", "1", wellbore_kind, other_id + ":"),
            ("osdu:master-data--Wellbore:wb-4", "1", wellbore_kind, "invalid-reference"),
            ("osdu:master-data--Wellbore:wb-5", "1", wellbore_kind, well_id + ":88"),
            ("osdu:master-data--Wellbore:wb-6", "1", wellbore_kind, None),
        ], "id string, version string, kind string, data__WellID string")

        class Registry:
            def direct_relationship_fields(self, kind):
                if kind != wellbore_kind:
                    return []
                return [{"field": "WellID", "targets": [
                    {"GroupType": "master-data", "EntityType": "Well"}
                ]}]

        result, bridge, bridge_tables = self.resolve_relationships(
            parent, source, Registry(), wellbore_kind, "fixture-run",
            version_strategy="merge",
        )
        rows = {row.id: row for row in result.collect()}
        self.assertNotIn("data__WellID__fk_id", result.columns)
        self.assertNotIn("data__WellID__fk_version", result.columns)
        self.assertEqual(rows["osdu:master-data--Wellbore:wb-1"]["data__WellID"], well_id + ":")
        self.assertEqual(rows["osdu:master-data--Wellbore:wb-2"]["data__WellID"], well_id + ":0008")
        self.assertEqual(result.count(), parent.count())
        self.assertEqual(len(bridge_tables), 1)
        self.assertEqual(
            bridge_tables,
            ["osdu_wks_wellbore__wellid__master_data_well"],
        )

        bridge_rows = {row.source_record_key.rsplit(":", 1)[0]: row for row in bridge.collect()}
        self.assertEqual(set(bridge_rows), {
            "osdu:master-data--Wellbore:wb-1",
            "osdu:master-data--Wellbore:wb-2",
            "osdu:master-data--Wellbore:wb-3",
        })
        self.assertEqual(
            bridge_rows["osdu:master-data--Wellbore:wb-1"].status,
            "target_deleted",
        )
        self.assertEqual(
            bridge_rows["osdu:master-data--Wellbore:wb-2"].target_record_key.rsplit(":", 1)[1],
            "0008",
        )
        self.assertEqual(
            bridge_rows["osdu:master-data--Wellbore:wb-2"].status,
            "resolved",
        )
        self.assertEqual(
            bridge_rows["osdu:master-data--Wellbore:wb-1"].target_record_key.rsplit(":", 1)[0],
            bridge_rows["osdu:master-data--Wellbore:wb-2"].target_record_key.rsplit(":", 1)[0],
        )
        self.assertTrue(all(row.run_id == "fixture-run" for row in bridge_rows.values()))

    def test_distinct_relationship_paths_get_distinct_bridge_tables(self):
        well_id = "osdu:master-data--Well:well-a"
        kind = "osdu:wks:master-data--Wellbore:1.0.0"
        source = self.spark.createDataFrame(
            [(well_id, "1", "osdu:wks:master-data--Well:1.0.0", True)],
            "id string, version string, kind string, isActive boolean",
        )
        parent = self.spark.createDataFrame(
            [("wb", "1", kind, well_id + ":", well_id + ":")],
            "id string, version string, kind string, data__WellID string, data__ParentWellID string",
        )

        class Registry:
            def direct_relationship_fields(self, kind):
                return [
                    {"field": field, "targets": [{"GroupType": "master-data", "EntityType": "Well"}]}
                    for field in ("WellID", "ParentWellID")
                ]

        result, bridge, bridge_tables = self.resolve_relationships(
            parent, source, Registry(), kind, "fixture-run"
        )
        self.assertEqual(result.columns, parent.columns)
        self.assertEqual(len(bridge_tables), 2)
        self.assertEqual({row._relationship_bridge_table for row in bridge.collect()}, {
            self.bridge_table_name(kind, f"data__{field}", "master-data--Well")
            for field in ("WellID", "ParentWellID")
        })
        self.assertEqual(
            {row._relationship_bridge_table for row in bridge.collect()},
            set(bridge_tables),
        )
        analyzed_plan = bridge._jdf.queryExecution().analyzed()

        def join_count(plan):
            children = plan.children()
            return int(plan.nodeName() == "Join") + sum(
                join_count(children.apply(index))
                for index in range(children.size())
            )

        self.assertEqual(join_count(analyzed_plan), 1)

    def test_overlapping_target_sets_preserve_exact_and_latest_resolution(self):
        kind = "osdu:wks:master-data--Wellbore:1.0.0"
        well_id = "osdu:master-data--Well:well-a"
        bore_id = "osdu:master-data--Wellbore:bore-a"
        well_kind = "osdu:wks:master-data--Well:1.0.0"
        huge_version = "1" + "0" * 45
        source = self.spark.createDataFrame([
            (well_id, "000", well_kind, True),
            (well_id, "9" * 44, well_kind, True),
            (well_id, huge_version, well_kind, False),
            (well_id, "not-a-version", well_kind, True),
            (bore_id, "2", kind, None),
            ("osdu:master-data--Other:other-a", "1", "osdu:wks:master-data--Other:1.0.0", True),
        ], "id string, version string, kind string, isActive boolean")
        parent = self.spark.createDataFrame([
            ("source-a", "1", kind, well_id + ":", well_id + ":0000", bore_id + ":"),
            ("source-b", "2", kind, bore_id + ":", "invalid", well_id + ":" + huge_version),
            ("source-c", "1", kind, None, well_id + ":7", "osdu:master-data--Other:other-a:"),
        ], "id string, version string, kind string, data__WellID string, data__ExactID string, data__EitherID string")

        class Registry:
            def direct_relationship_fields(self, kind):
                well = {"GroupType": "master-data", "EntityType": "Well"}
                bore = {"GroupType": "master-data", "EntityType": "Wellbore"}
                return [
                    {"field": "WellID", "targets": [well]},
                    {"field": "ExactID", "targets": [well]},
                    {"field": "EitherID", "targets": [bore, well]},
                ]

        result, bridge, tables = self.resolve_relationships(parent, source, Registry(), kind)
        self.assertIs(result, parent)
        self.assertEqual(len(tables), 4)
        rows = bridge.collect()
        self.assertEqual(len(rows), 4)
        self.assertEqual(
            {(r.source_record_key.rsplit(":", 1)[0], r._relationship_bridge_table, *r.target_record_key.rsplit(":", 1), r.status) for r in rows},
            {
                (source, self.bridge_table_name(kind, path, target.split(":")[1]), target, version, status)
                for source, path, target, version, status in (
                    ("source-a", "data__WellID", well_id, huge_version, "target_deleted"),
                    ("source-a", "data__ExactID", well_id, "000", "resolved"),
                    ("source-a", "data__EitherID", bore_id, "2", "resolved"),
                    ("source-b", "data__EitherID", well_id, huge_version, "target_deleted"),
                )
            },
        )
        for row in rows:
            self.assertEqual(row.source_record_key.rsplit(":", 1)[1], "2" if row.source_record_key.rsplit(":", 1)[0] == "source-b" else "1")
            self.assertEqual(row.source_kind, kind)

    def test_empty_references_retain_planned_tables_and_bridge_schema(self):
        kind = "osdu:wks:master-data--Wellbore:1.0.0"
        source = self.spark.createDataFrame([], "id string, version string, kind string")
        parent = self.spark.createDataFrame(
            [("source", "1", kind, None)],
            "id string, version string, kind string, data__WellID string",
        )

        class Registry:
            def direct_relationship_fields(self, kind):
                return [{"field": "WellID", "targets": [
                    {"GroupType": "master-data", "EntityType": "Well"},
                ]}]

        result, bridge, tables = self.resolve_relationships(parent, source, Registry(), kind)
        self.assertIs(result, parent)
        self.assertEqual(len(tables), 1)
        self.assertEqual(bridge.count(), 0)
        self.assertEqual(bridge.drop("_relationship_bridge_table").columns, [
            "source_record_key", "target_record_key", "source_kind", "status", "run_id",
        ])
        self.assertTrue(all(isinstance(field.dataType, T.StringType) for field in bridge.schema.fields))

    def test_duplicate_target_identity_is_rejected(self):
        target_kind = "osdu:wks:master-data--Well:1.0.0"
        target_id = "osdu:master-data--Well:well-a"
        source = self.spark.createDataFrame([
            (target_id, "1", target_kind),
            (target_id, "01", target_kind),
        ], "id string, version string, kind string")
        parent = self.spark.createDataFrame(
            [("record", "osdu:master-data--Well:well-a:1")], "id string, data__WellID string"
        )

        class Registry:
            def direct_relationship_fields(self, kind):
                return [{"field": "WellID", "targets": [
                    {"GroupType": "master-data", "EntityType": "Well"}
                ]}]

        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.resolve_relationships(parent, source, Registry(), "osdu:wks:master-data--Wellbore:1.0.0")

    def test_reference_replay_preserves_pinning_across_target_activation_transitions(self):
        target_kind = "osdu:wks:master-data--Well:1.0.0"
        source_kind = "osdu:wks:master-data--Wellbore:1.0.0"
        target_id = "test:master-data--Well:target"
        parent = self.spark.createDataFrame([
            ("latest", "1", source_kind, target_id + ":"),
            ("pinned", "1", source_kind, target_id + ":0001"),
        ], "id string, version string, kind string, data__WellID string")

        class Registry:
            def direct_relationship_fields(self, kind):
                return [{"field": "WellID", "targets": [{"GroupType": "master-data", "EntityType": "Well"}]}]

        for targets, expected in (
            ([], set()),
            ([("1", True)], {("latest", "1", "resolved"), ("pinned", "1", "resolved")}),
            ([("1", False), ("2", True)], {("latest", "2", "resolved"), ("pinned", "1", "target_deleted")}),
            ([("1", True), ("2", False)], {("latest", "2", "target_deleted"), ("pinned", "1", "resolved")}),
            ([("2", True)], {("latest", "2", "resolved")}),
        ):
            with self.subTest(targets=targets):
                source = self.spark.createDataFrame(
                    [(target_id, version, target_kind, active) for version, active in targets],
                    "id string, version string, kind string, isActive boolean",
                )
                result, bridge, _ = self.resolve_relationships(
                    parent, source, Registry(), source_kind, version_strategy="merge",
                )
                self.assertIs(result, parent)
                self.assertEqual(
                    expected, {(record.source_record_key.rsplit(":", 1)[0], record.target_record_key.rsplit(":", 1)[1], record.status) for record in bridge.collect()},
                )


if __name__ == "__main__":
    unittest.main()
