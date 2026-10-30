"""Notebook runs on local Spark/Delta for the modes the default run does not reach.

Covers upsert with the incremental watermark and inactive-record deletes, wide output with versioned tables and
data-quality issues, schema inference for kinds missing from the schema service, the dry-run profile, and the
authentication and Fabric-context helpers. Bronze rows are synthetic and the schema service is stubbed.
"""

import copy
import json
import os
import unittest
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

try:
    import requests
except ModuleNotFoundError as error:
    if error.name != "requests":
        raise
    requests = None

from notebook_runner import run_notebook
from test_notebook_integration import WELL_KIND, WELL_SCHEMA, NotebookIntegrationBase
from tno_bronze import BRONZE_DDL

WELLBORE_KIND = "osdu:wks:master-data--Wellbore:1.0.0"
FACILITY_TYPE_KIND = "osdu:wks:reference-data--FacilityType:1.0.0"
EARLIER = datetime(2026, 1, 1, tzinfo=timezone.utc)
LATER = datetime(2026, 2, 1, tzinfo=timezone.utc)
LATEST = datetime(2026, 3, 1, tzinfo=timezone.utc)


def row(record_id, version, payload, kind=WELL_KIND, active=True, ingest=EARLIER):
    envelope = json.dumps({"data": payload, "meta": None, "modifyUser": "buildagent", "modifyTime": 1760000000000})
    return (envelope, None, record_id, version, kind, None, None, None, "buildagent", None, None, None,
            ingest, None, None, None, active)


def write_bronze(spark, rows, mode="overwrite"):
    writer = spark.createDataFrame(rows, BRONZE_DDL).write.format("delta").mode(mode)
    writer.saveAsTable("osducatalog")


def schema_stub(missing_kinds=()):
    def serve(url, context, timeout=None, session=None):
        if "latestVersion" in url:
            return []
        if any(kind in url for kind in missing_kinds):
            response = requests.Response()
            response.status_code = 404
            raise requests.HTTPError(f"HTTP 404 for {context}", response=response)
        return {"schema": WELL_SCHEMA}
    return serve


def run(spark, settings, missing_kinds=(), capture=None, configure=None):
    def stub_services(namespace):
        if capture is not None:
            capture["get_adme_access_token"] = namespace["get_adme_access_token"]
            capture["namespace"] = namespace
        namespace["get_adme_access_token"] = lambda: "unit-test-token"
        namespace["_adme_schema_get_json"] = schema_stub(missing_kinds)
        namespace["_table_path_uri"] = lambda table: f"file:///nonexistent-delta-path/{table}"
        if configure is not None:
            configure(namespace)

    return run_notebook(spark, {
        "ADME_ENDPOINT": "https://adme.example.test",
        "ADME_DATA_PARTITION_ID": "test-partition",
        "ADME_AUTH_METHOD": "MI",
        "ALLOW_OVERWRITE": True,
        **settings,
    }, before_pipeline=stub_services)


def keys(frame):
    return {(r["id"], r["version"]) for r in frame.select("id", "version").distinct().collect()}


def bridge_tables(namespace, kind=None):
    return {
        table for result in namespace["results"] if kind is None or result.kind == kind
        for table in result.child_tables
        if namespace["_is_relationship_bridge"](table, result.parent_table)
    }


class UpsertWatermarkTests(NotebookIntegrationBase):
    SETTINGS = {
        "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": "up_",
        "WRITE_MODE": "upsert", "VERSION_STRATEGY": "merge", "INCREMENTAL_WATERMARK_MODE": "auto",
    }

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [
            row("test:well:1", "1", {
                "FacilityName": "Alpha",
                "FacilityTypeID": "test:reference-data--FacilityType:facility-a:",
                "NameAliases": [{"AliasName": "A1"}],
            }),
            row("test:well:2", "1", {
                "FacilityName": "Beta",
                "FacilityTypeID": "test:reference-data--FacilityType:facility-b:",
                "NameAliases": [{"AliasName": "B1"}],
            }),
            row("test:well:3", "1", {"FacilityName": "Gamma", "NameAliases": []}),
            row("test:reference-data--FacilityType:facility-a", "1", {"Name": "Facility A"},
                kind=FACILITY_TYPE_KIND),
            row("test:reference-data--FacilityType:facility-b", "1", {"Name": "Facility B"},
                kind=FACILITY_TYPE_KIND),
        ])
        initial = run(cls.spark, cls.SETTINGS)
        cls.first_keys = keys(cls.spark.table("up_osdu_wks_well"))
        cls.bridge_table = next(iter(bridge_tables(initial)))
        cls.first_bridge_rows = {
            tuple(record["source_record_key"].rsplit(":", 1)): (record["target_record_key"].rsplit(":", 1)[0], record["status"])
            for record in cls.spark.table(cls.bridge_table).collect()
        }
        cls.spark.createDataFrame([
            row("test:well:1", "2", {
                "FacilityName": "Alpha renamed",
                "FacilityTypeID": "test:reference-data--FacilityType:facility-b:",
                "NameAliases": [{"AliasName": "A2"}],
            }, ingest=LATER),
            row("test:well:4", "1", {"FacilityName": "Delta", "NameAliases": []}, ingest=LATER),
        ], BRONZE_DDL).write.format("delta").mode("append").saveAsTable("osducatalog")
        cls.spark.sql(f"UPDATE osducatalog SET isActive = false, ingestTime = TIMESTAMP '{LATER:%Y-%m-%d %H:%M:%S}' "
                      "WHERE id = 'test:well:2'")
        cls.namespace = run(cls.spark, cls.SETTINGS)
        cls.second_keys = keys(cls.spark.table("up_osdu_wks_well"))
        cls.spark.createDataFrame([
            row("test:well:5", "1", {"FacilityName": "Epsilon", "NameAliases": []}, ingest=LATER),
        ], BRONZE_DDL).write.format("delta").mode("append").saveAsTable("osducatalog")
        run(cls.spark, cls.SETTINGS)
        cls.third_keys = keys(cls.spark.table("up_osdu_wks_well"))

    def test_first_run_publishes_every_active_record(self):
        self.assertEqual({("test:well:1", "1"), ("test:well:2", "1"), ("test:well:3", "1")}, self.first_keys)

    def test_second_run_merges_new_versions_and_records(self):
        self.assertEqual(
            {("test:well:1", "1"), ("test:well:1", "2"), ("test:well:3", "1"), ("test:well:4", "1")},
            self.second_keys)

    def test_rows_at_the_watermark_boundary_are_reprocessed(self):
        self.assertIn(("test:well:5", "1"), self.third_keys)

    def test_deactivated_record_is_deleted_from_parent_and_child_tables(self):
        self.assertNotIn("test:well:2", {r["id"] for r in self.read("up_osdu_wks_well").collect()})
        aliases = {r["AliasName"] if "AliasName" in r.asDict() else r["data__AliasName"]
                   for r in self.read("up_osdu_wks_well___namealiases").collect()}
        self.assertNotIn("B1", aliases)
        self.assertIn("A2", aliases)

    def test_watermark_state_records_the_latest_ingest_time(self):
        state = self.read("silver_incremental_state").orderBy("updated_at").collect()
        self.assertEqual(
            ["2026-01-01 00:00:00", "2026-02-01 00:00:00", "2026-02-01 00:00:00"],
            [r["watermark_value"] for r in state],
        )


    def test_watermark_upsert_refreshes_bridges_and_deletes_inactive_sources(self):
        self.assertEqual(
            {
                ("test:well:1", "1"): ("test:reference-data--FacilityType:facility-a", "resolved"),
                ("test:well:2", "1"): ("test:reference-data--FacilityType:facility-b", "resolved"),
            },
            self.first_bridge_rows,
        )
        bridge_rows = {
            tuple(record["source_record_key"].rsplit(":", 1)): (record["target_record_key"].rsplit(":", 1)[0], record["status"])
            for record in self.read(self.bridge_table).collect()
        }
        self.assertEqual(
            {
                ("test:well:1", "1"): ("test:reference-data--FacilityType:facility-a", "resolved"),
                ("test:well:1", "2"): ("test:reference-data--FacilityType:facility-b", "resolved"),
            },
            bridge_rows,
        )
        self.assertIn(self.bridge_table, self.namespace["results"][0].child_tables)


class DocumentArrayIdentityTests(NotebookIntegrationBase):
    def test_refresh_and_upsert_preserve_array_ids_in_normalized_and_wide_outputs(self):
        kind = "osdu:wks:work-product-component--Document:1.0.0"
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "kind": {"type": "string"},
                "data": {
                    "type": "object",
                    "properties": {
                        "Items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    name: {"type": "string"} for name in ("ID", "Version", "Ordinal", "source_record_key")
                                },
                            },
                        },
                    },
                },
            },
        }

        def configure(namespace):
            namespace["_adme_schema_get_json"] = (
                lambda url, *args, **kwargs: [] if "latestVersion" in url else {"schema": schema}
            )

        for output_mode in ("normalized", "wide"):
            prefix = f"document_{output_mode}_"
            settings = {
                "RUN_PROFILE": "execute", "KINDS": [kind], "TABLE_PREFIX": prefix,
                "OUTPUT_MODE": output_mode, "VERSION_STRATEGY": "merge",
                "INCREMENTAL_WATERMARK_MODE": "auto",
            }
            for write_mode, item_id, ingest in (
                ("full_refresh", "element-initial", EARLIER),
                ("upsert", "element-revised", LATER),
            ):
                with self.subTest(output_mode=output_mode, write_mode=write_mode):
                    write_bronze(self.spark, [
                        row("test:document:1", "9", {
                            "Items": [{"ID": item_id, "Version": "element-version", "Ordinal": "element-ordinal",
                                       "source_record_key": "element-reference"}],
                        }, kind=kind, ingest=ingest),
                    ])
                    namespace = run(self.spark, {**settings, "WRITE_MODE": write_mode}, configure=configure)
                    self.assertEqual(["success"], [result.status for result in namespace["results"]])
                    parent_table = prefix + "osdu_wks_document"
                    parent = self.read(parent_table)
                    self.assertEqual(keys(parent), {("test:document:1", "9")})
                    output = self.read(parent_table + "___items") if output_mode == "normalized" else parent
                    records = output.collect()
                    self.assertEqual(len(records), 1)
                    self.assertEqual(len(output.columns), len({name.casefold() for name in output.columns}))
                    record = records[0].asDict()
                    self.assertEqual(record["id"], "test:document:1")
                    self.assertEqual(record["version"], "9")
                    self.assertEqual(record["record_key"], 'test:document:1:9')
                    self.assertEqual(parent.first()["record_key"], record["record_key"])
                    self.assertFalse(any(name.endswith("_record_key") and not name.endswith("source_record_key") for name in output.columns))
                    if output_mode == "normalized":
                        self.assertEqual(record["ordinal"], 0)
                    for expected in (item_id, "element-version", "element-ordinal", "element-reference"):
                        self.assertIn(expected, record.values())


class RelationshipAliasCollisionTests(NotebookIntegrationBase):
    def test_full_refresh_upsert_and_target_changes_keep_colliding_fields_distinct(self):
        target_a = "test:reference-data--FacilityType:facility-a"
        target_b = "test:reference-data--FacilityType:facility-b"
        schema = copy.deepcopy(WELL_SCHEMA)
        schema["properties"]["data"] = {
            "type": "object",
            "properties": {
                "Primary ID": {"type": "string"},
                **{
                    field: {
                        "type": "string",
                        "x-osdu-relationship": [
                            {"GroupType": "reference-data", "EntityType": "FacilityType"}
                        ],
                    }
                    for field in ("Primary-ID", "Primary_ID")
                },
            },
        }

        def configure(namespace):
            namespace["_adme_schema_get_json"] = (
                lambda url, *args, **kwargs: [] if "latestVersion" in url else {"schema": schema}
            )

        for output_mode, strategy in product(("normalized", "wide"), ("merge", "versioned_tables")):
            with self.subTest(output_mode=output_mode, strategy=strategy):
                prefix = f"alias_{output_mode}_{strategy}_"
                parent_table = prefix + "osdu_wks_well" + ("__v1_0_0" if strategy == "versioned_tables" else "")
                settings = {
                    "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": prefix,
                    "OUTPUT_MODE": output_mode, "VERSION_STRATEGY": strategy,
                    "INCREMENTAL_WATERMARK_MODE": "auto",
                }
                expected_tables = None
                for write_mode, targets, ingest in (
                    ("full_refresh", (target_a, target_b), EARLIER),
                    ("upsert", (target_b, target_a), LATER),
                ):
                    with self.subTest(write_mode=write_mode):
                        payload = {"Primary ID": "not-a-reference", "Primary-ID": targets[0] + ":",
                                   "Primary_ID": targets[1] + ":"}
                        write_bronze(self.spark, [
                            row("test:well:1", "1", payload, ingest=ingest),
                            row("test:well:boundary", "1", {}, active=False, ingest=LATEST),
                            row(target_a, "1", {"Name": "A"}, kind=FACILITY_TYPE_KIND),
                            row(target_b, "1", {"Name": "B"}, kind=FACILITY_TYPE_KIND),
                        ])
                        namespace = run(self.spark, {**settings, "WRITE_MODE": write_mode}, configure=configure)
                        self.assertEqual(["success"], [result.status for result in namespace["results"]])
                        tables = {
                            table.name for table in self.spark.catalog.listTables()
                            if namespace["_is_relationship_bridge"](table.name, parent_table)
                        }
                        self.assertEqual(len(tables), 2)
                        if expected_tables is not None:
                            self.assertEqual(tables, expected_tables)
                        expected_tables = tables
                        rows = [record for table in tables for record in self.read(table).collect()]
                        self.assertEqual(len(rows), 2)
                        self.assertEqual(
                            {(table, record.target_record_key.rsplit(":", 1)[0]) for table in tables for record in self.read(table).collect()},
                            {(namespace["relationship_bridge_table_name"](WELL_KIND, f"data__{field}", "reference-data--FacilityType", prefix, strategy), target)
                             for field, target in zip(("Primary-ID", "Primary_ID"), targets)},
                        )
                        self.assertTrue(tables.issubset(set(namespace["results"][0].child_tables)))
                        parent = self.read(parent_table).first().asDict()
                        for reference in payload.values():
                            self.assertIn(reference, parent.values())

                parent_version = self.spark.sql(f"DESCRIBE HISTORY `{parent_table}` LIMIT 1").first()["version"]
                parent_rows = self.read(parent_table).collect()
                source_rows = self.read("osducatalog").where(f"kind = '{WELL_KIND}'").collect()
                write_bronze(self.spark, [
                    row(target_a, "2", {"Name": "A"}, kind=FACILITY_TYPE_KIND, ingest=LATEST),
                    row(target_b, "3", {"Name": "B"}, kind=FACILITY_TYPE_KIND, active=False, ingest=LATEST),
                ], mode="append")
                namespace = run(self.spark, {**settings, "WRITE_MODE": "upsert"}, configure=configure)
                self.assertEqual(["success"], [result.status for result in namespace["results"]])
                self.assertEqual(
                    expected_tables,
                    bridge_tables(namespace),
                )
                bridge_rows = [record for table in expected_tables for record in self.read(table).collect()]
                self.assertEqual(len(bridge_rows), 2)
                self.assertEqual(
                    {(table, *record.target_record_key.rsplit(":", 1), record.status)
                     for table in expected_tables for record in self.read(table).collect()},
                    {(namespace["relationship_bridge_table_name"](WELL_KIND, f"data__{field}", "reference-data--FacilityType", prefix, strategy), target, version, status)
                     for field, target, version, status in (
                         ("Primary-ID", target_b, "3", "target_deleted"),
                         ("Primary_ID", target_a, "2", "resolved"),
                     )},
                )
                self.assertEqual(
                    parent_version,
                    self.spark.sql(f"DESCRIBE HISTORY `{parent_table}` LIMIT 1").first()["version"],
                )
                self.assertCountEqual(parent_rows, self.read(parent_table).collect())
                self.assertCountEqual(source_rows, self.read("osducatalog").where(f"kind = '{WELL_KIND}'").collect())


class LongArrayTableNameTests(NotebookIntegrationBase):
    def test_bounded_arrays_publish_update_and_delete_inactive_sources(self):
        kind = f"osdu:wks:master-data--W{'x' * 110}:1.0.0"
        prefix = "arr_"
        schema = copy.deepcopy(WELL_SCHEMA)
        array_schema = {
            "type": "array",
            "items": {"type": "object", "properties": {"label": {"type": "string"}}},
        }
        schema["properties"]["data"] = {
            "type": "object",
            "properties": {
                "x": array_schema,
                "SpatialLocation": {
                    "type": "object",
                    "properties": {
                        "AsIngestedCoordinates": {
                            "type": "object",
                            "properties": {"features": array_schema},
                        },
                    },
                },
            },
        }

        def configure(namespace):
            namespace["_adme_schema_get_json"] = (
                lambda url, *args, **kwargs: [] if "latestVersion" in url else {"schema": schema}
            )

        settings = {
            "RUN_PROFILE": "execute", "KINDS": [kind], "TABLE_PREFIX": prefix,
            "VERSION_STRATEGY": "merge", "INCREMENTAL_WATERMARK_MODE": "auto",
            "CREATE_EMPTY_CHILD_TABLES": True, "WRITE_RELATIONSHIP_BRIDGES": False,
        }
        expected_tables = None
        for mode, value, active, ingest in (
            ("full_refresh", "alpha", True, EARLIER),
            ("upsert", "beta", True, LATER),
            ("upsert", "beta", False, LATEST),
        ):
            with self.subTest(mode=mode, active=active):
                payload = {
                    "x": [{"label": value}],
                    "SpatialLocation": {"AsIngestedCoordinates": {"features": [{"label": value}]}},
                }
                write_bronze(self.spark, [row("test:well:bounded", "0007", payload, kind=kind, active=active, ingest=ingest)])
                namespace = run(self.spark, {**settings, "WRITE_MODE": mode}, configure=configure)
                self.assertTrue(all(result.status != "failed" for result in namespace["results"]))
                parent = namespace["results"][0].parent_table
                self.assertEqual(len(parent), 124)
                tables = {
                    namespace["child_table_name"](parent, source)
                    for source in ("data__x", "data__SpatialLocation__AsIngestedCoordinates__features")
                }
                self.assertEqual(len(tables), 2)
                self.assertTrue(all(len(table) == 128 for table in tables))
                self.assertIn(parent + "___x", tables)
                self.assertTrue(any(not table.startswith(parent + "___") for table in tables))
                if expected_tables is not None:
                    self.assertEqual(tables, expected_tables)
                expected_tables = tables
                self.assertEqual(self.read(parent).count(), int(active))
                for table in tables:
                    records = self.read(table).collect()
                    self.assertEqual(
                        {(record.record_key, record.ordinal, record.label) for record in records},
                        {("test:well:bounded:0007", 0, value)} if active else set(),
                    )
                self.assertTrue(tables.issubset(set(namespace["_validated_historical_outputs"](
                    self.spark, parent, namespace["workspace_id"], namespace["lakehouse_id"],
                ))))


class LongRelationshipTableNameTests(NotebookIntegrationBase):
    def test_bounded_bridges_publish_update_and_delete_inactive_sources(self):
        kinds = {
            "field": "osdu:wks:work-product-component--WellboreMarkerSet:1.0.0",
            "source": f"osdu:wks:work-product-component--WellboreMarkerSet{'Extension' * 8}:1.0.0",
        }
        fields = ("ResourceCurationStatus", "ResourceLifecycleStatus")
        schema = copy.deepcopy(WELL_SCHEMA)
        schema["properties"]["data"] = {
            "type": "object",
            "properties": {
                field: {
                    "type": "string",
                    "x-osdu-relationship": [{"GroupType": "reference-data", "EntityType": field}],
                }
                for field in fields
            },
        }

        def configure(namespace):
            namespace["_adme_schema_get_json"] = (
                lambda url, *args, **kwargs: [] if "latestVersion" in url else {"schema": schema}
            )

        for (case, kind), strategy in product(kinds.items(), ("merge", "versioned_tables")):
            with self.subTest(case=case, strategy=strategy):
                prefix = f"bound_{case}_{strategy[0]}_"
                settings = {
                    "RUN_PROFILE": "execute", "KINDS": [kind], "TABLE_PREFIX": prefix,
                    "VERSION_STRATEGY": strategy, "INCREMENTAL_WATERMARK_MODE": "auto",
                }
                expected_tables = None
                for mode, suffix, active, ingest in (
                    ("full_refresh", "a", True, EARLIER),
                    ("upsert", "b", True, LATER),
                    ("upsert", "b", False, LATEST),
                ):
                    with self.subTest(mode=mode, active=active):
                        references = {field: f"test:reference-data--{field}:{suffix}:" for field in fields}
                        write_bronze(self.spark, [
                            row("test:marker:1", "1", references, kind=kind, active=active, ingest=ingest),
                            *[
                                row(f"test:reference-data--{field}:{target}", "1", {"Name": target},
                                    kind=f"osdu:wks:reference-data--{field}:1.0.0")
                                for field in fields for target in ("a", "b")
                            ],
                        ])
                        namespace = run(self.spark, {**settings, "WRITE_MODE": mode}, configure=configure)
                        self.assertTrue(namespace["results"])
                        self.assertTrue(all(result.status != "failed" for result in namespace["results"]))
                        registry = namespace["SchemaRegistry"].from_dict({kind: schema})
                        planned = set(namespace["relationship_bridge_tables_for_kind"](kind, registry, prefix, strategy))
                        self.assertEqual(len(planned), 2)
                        self.assertTrue(all(len(name) <= 128 for name in planned))
                        parent_table = namespace["results"][0].parent_table
                        ownership_prefix = namespace["_relationship_table_prefix"](parent_table)
                        if case == "source":
                            self.assertEqual(len(ownership_prefix), 109)
                            self.assertTrue(all(len(name) == 128 for name in planned))
                        self.assertTrue(all(name.startswith(ownership_prefix) for name in planned))
                        self.assertEqual(
                            planned,
                            {table.name for table in self.spark.catalog.listTables()
                             if namespace["_is_relationship_bridge"](table.name, parent_table)},
                        )
                        if expected_tables is not None:
                            self.assertEqual(planned, expected_tables)
                        expected_tables = planned
                        bridge_rows = [record for table in planned for record in self.read(table).collect()]
                        self.assertEqual(len(bridge_rows), 2 if active else 0)
                        self.assertEqual(
                            {(table, record.target_record_key.rsplit(":", 1)[0]) for table in planned for record in self.read(table).collect()},
                            {(namespace["relationship_bridge_table_name"](kind, f"data__{field}", reference.split(":")[1], prefix, strategy), reference[:-1])
                             for field, reference in references.items()} if active else set(),
                        )
                        self.assertTrue(planned.issubset(set(namespace["results"][0].child_tables)))


class WatermarkedRelationshipBridgeTests(NotebookIntegrationBase):
    KIND = "osdu:wks:master-data--Wellbore:1.0.0"
    TARGET_KIND = "osdu:wks:reference-data--FacilityType:1.0.0"
    TARGET_ID = "test:reference-data--FacilityType:well"
    OTHER_TARGET_ID = "test:reference-data--FacilityType:bore"
    SETTINGS = {
        "RUN_PROFILE": "execute", "KINDS": [KIND], "TABLE_PREFIX": "wmbridge_",
        "WRITE_MODE": "upsert", "VERSION_STRATEGY": "merge", "INCREMENTAL_WATERMARK_MODE": "auto",
    }

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [
            row("test:wellbore:1", "1", {"FacilityTypeID": cls.TARGET_ID + ":"}, kind=cls.KIND),
            row(cls.TARGET_ID, "1", {"FacilityName": "Well"}, kind=cls.TARGET_KIND),
            row(cls.OTHER_TARGET_ID, "1", {"FacilityName": "Bore"}, kind=cls.TARGET_KIND),
        ])
        initial = run(cls.spark, cls.SETTINGS)
        cls.bridge_table = next(iter(bridge_tables(initial)))
        payload = row("test:wellbore:1", "1", {"FacilityTypeID": cls.OTHER_TARGET_ID + ":"}, kind=cls.KIND)[0]
        cls.spark.sql(
            f"UPDATE osducatalog SET data = '{payload}', ingestTime = TIMESTAMP '{LATER:%Y-%m-%d %H:%M:%S}' "
            "WHERE id = 'test:wellbore:1' AND version = '1'"
        )
        cls.update_results = run(cls.spark, cls.SETTINGS)["results"]
        cls.bridge_after_source_update = cls.spark.table(cls.bridge_table).collect()
        cls.spark.sql(
            f"UPDATE osducatalog SET isActive = false, ingestTime = TIMESTAMP '{LATEST:%Y-%m-%d %H:%M:%S}' "
            "WHERE id = 'test:wellbore:1' AND version = '1'"
        )
        cls.delete_results = run(cls.spark, cls.SETTINGS)["results"]

    def test_watermarked_source_change_replaces_its_bridge_row(self):
        self.assertEqual(["success"], [result.status for result in self.update_results])
        rows = self.bridge_after_source_update
        self.assertEqual(1, len(rows))
        self.assertEqual(
            (self.OTHER_TARGET_ID, "1", "resolved"),
            (*rows[0]["target_record_key"].rsplit(":", 1), rows[0]["status"]),
        )
        self.assertEqual(["test:wellbore:1", "1"], rows[0]["source_record_key"].rsplit(":", 1))

    def test_watermarked_inactive_source_deletes_its_bridge_row(self):
        self.assertEqual(["success"], [result.status for result in self.delete_results])
        self.assertEqual(0, self.read(self.bridge_table).count())


class WatermarkedRelationshipBridgeDisabledTests(NotebookIntegrationBase):
    KIND = "osdu:wks:master-data--Wellbore:2.0.0"
    TARGET_KIND = "osdu:wks:reference-data--FacilityType:1.0.0"
    TARGET_ID = "test:reference-data--FacilityType:disabled"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.settings = {
            "RUN_PROFILE": "execute", "KINDS": [cls.KIND], "TABLE_PREFIX": "wmnobridge_",
            "WRITE_MODE": "upsert", "VERSION_STRATEGY": "merge", "INCREMENTAL_WATERMARK_MODE": "auto",
            "WRITE_RELATIONSHIP_BRIDGES": True,
        }
        write_bronze(cls.spark, [
            row("test:wellbore:disabled", "1", {"FacilityTypeID": cls.TARGET_ID + ":"}, kind=cls.KIND),
            row(cls.TARGET_ID, "1", {"FacilityName": "Disabled"}, kind=cls.TARGET_KIND),
        ])
        initial = run(cls.spark, cls.settings)
        cls.bridge_table = next(iter(bridge_tables(initial)))
        cls.settings["WRITE_RELATIONSHIP_BRIDGES"] = False
        cls.spark.createDataFrame([
            row("test:wellbore:disabled", "2", {"FacilityTypeID": "test:reference-data--FacilityType:other:"},
                kind=cls.KIND, ingest=LATER),
        ], BRONZE_DDL).write.format("delta").mode("append").saveAsTable("osducatalog")
        run(cls.spark, cls.settings)

    def test_disabling_bridges_keeps_parent_updates_and_does_not_delete_existing_bridge_tables(self):
        parent = self.read("wmnobridge_osdu_wks_wellbore")
        current = parent.where("version = '2'").first()
        self.assertEqual("test:reference-data--FacilityType:other:", current["data__FacilityTypeID"])
        bridge_rows = self.read(self.bridge_table).collect()
        self.assertEqual(1, len(bridge_rows))
        self.assertEqual([self.TARGET_ID, "1"], bridge_rows[0]["target_record_key"].rsplit(":", 1))


class WatermarkedBridgeVersionRetentionTests(NotebookIntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        target_kind = "osdu:wks:reference-data--FacilityType:1.0.0"
        original_target = "test:reference-data--FacilityType:original"
        deleted_source_target = "test:reference-data--FacilityType:deleted-source"
        replacement_target = "test:reference-data--FacilityType:replacement"
        write_bronze(cls.spark, [
            row("test:well:1", "1", {"FacilityName": "Alpha", "FacilityTypeID": original_target + ":"}),
            row("test:well:2", "1", {"FacilityName": "Beta", "FacilityTypeID": deleted_source_target + ":"}),
            row(original_target, "1", {"Name": "Original"}, kind=target_kind),
            row(deleted_source_target, "1", {"Name": "Deleted source target"}, kind=target_kind),
            row(replacement_target, "1", {"Name": "Replacement"}, kind=target_kind),
        ])
        cls.settings = {
            "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": "wm_bridge_",
            "WRITE_MODE": "upsert", "VERSION_STRATEGY": "merge", "INCREMENTAL_WATERMARK_MODE": "auto",
        }
        run(cls.spark, cls.settings)
        cls.spark.createDataFrame([
            row("test:well:1", "2", {"FacilityName": "Alpha renamed", "FacilityTypeID": replacement_target + ":"},
                ingest=LATER),
        ], BRONZE_DDL).write.format("delta").mode("append").saveAsTable("osducatalog")
        cls.spark.sql(
            f"UPDATE osducatalog SET isActive = false, "
            f"ingestTime = TIMESTAMP '{LATER:%Y-%m-%d %H:%M:%S}' WHERE id = 'test:well:2'"
        )
        cls.namespace = run(cls.spark, cls.settings)
        bridges = bridge_tables(cls.namespace)
        if len(bridges) != 1:
            raise AssertionError(f"Expected one relationship bridge table, found {bridges}")
        cls.bridge_table = next(iter(bridges))

    def test_watermark_upsert_replaces_changed_source_bridge_rows_and_removes_inactive_sources(self):
        rows = self.read(self.bridge_table).collect()
        self.assertEqual(
            {
                ("test:well:1", "1", "test:reference-data--FacilityType:original"),
                ("test:well:1", "2", "test:reference-data--FacilityType:replacement"),
            },
            {(*row.source_record_key.rsplit(":", 1), row.target_record_key.rsplit(":", 1)[0]) for row in rows},
        )
        self.assertTrue(all(row.status == "resolved" for row in rows))
        self.assertTrue(all(row.target_record_key.rsplit(":", 1)[1] == "1" for row in rows))


class WideVersionedOutputTests(NotebookIntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [
            row("test:well:1", "1", {"FacilityName": "Alpha", "Unmodelled": "x",
                                     "NameAliases": [{"AliasName": "A1"}, {"AliasName": "A2"}]}),
            row("test:well:2", "1", {"FacilityName": "Beta", "NameAliases": []}),
            row("test:well:2", "1", {"FacilityName": "Beta", "NameAliases": []}),
        ])
        cls.namespace = run(cls.spark, {
            "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": "wide_", "OUTPUT_MODE": "wide",
            "WRITE_MODE": "full_refresh", "VERSION_STRATEGY": "versioned_tables",
        })

    def test_versioned_wide_table_holds_one_row_per_record(self):
        table = self.read("wide_osdu_wks_well__v1_0_0")
        self.assertEqual({"test:well:1", "test:well:2"}, {r["id"] for r in table.collect()})

    def test_child_arrays_are_reassembled_into_the_wide_row(self):
        table = self.read("wide_osdu_wks_well__v1_0_0")
        alias_columns = [c for c in table.columns if "namealiases" in c.lower()]
        self.assertTrue(alias_columns, table.columns)
        first = table.filter("id = 'test:well:1'").collect()[0][alias_columns[0]]
        self.assertEqual(2, len(first))

    def test_data_quality_issue_is_recorded_for_duplicate_merge_key(self):
        issues = self.read("silver_data_quality_issues").filter("check_name = 'duplicate_merge_key'").collect()
        self.assertEqual(["test:well:2"], [r["source_id"] for r in issues])
        self.assertIn("appears 2 times", issues[0]["issue_detail"])


class BridgeWatermarkLifecycleTests(NotebookIntegrationBase):
    TARGET_KIND = "osdu:wks:reference-data--FacilityType:1.0.0"
    TARGET_PREFIX = "test:reference-data--FacilityType:"

    def source(self, record_id, version, target, **kwargs):
        payload = {"FacilityName": record_id, "FacilityTypeID": f"{self.TARGET_PREFIX}{target}:"}
        return row(record_id, version, payload, **kwargs)

    def target(self, name, **kwargs):
        return row(f"{self.TARGET_PREFIX}{name}", "1", {"Name": name}, kind=self.TARGET_KIND, **kwargs)

    def build(self, **overrides):
        settings = {
            "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": "life_",
            "WRITE_MODE": "upsert", "VERSION_STRATEGY": "merge", "INCREMENTAL_WATERMARK_MODE": "auto",
        }
        settings.update(overrides)
        return run(self.spark, settings)

    def append(self, rows):
        self.spark.createDataFrame(rows, BRONZE_DDL).write.format("delta").mode("append").saveAsTable("osducatalog")

    def edges(self, bridge):
        prefix_length = len(self.TARGET_PREFIX)
        return sorted(
            (*r["source_record_key"].rsplit(":", 1), r["target_record_key"].rsplit(":", 1)[0][prefix_length:], r["status"])
            for r in self.read(bridge).collect()
        )

    def test_bridge_rows_follow_changed_sources_across_watermarked_runs(self):
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        write_bronze(self.spark, [
            self.source("s1", "1", "a"), self.source("s2", "1", "b"),
            self.target("a"), self.target("b"), self.target("c"),
        ])
        namespace = self.build()
        bridge = next(iter(bridge_tables(namespace)))
        self.assertEqual([("s1", "1", "a", "resolved"), ("s2", "1", "b", "resolved")], self.edges(bridge))

        self.build()
        self.assertEqual([("s1", "1", "a", "resolved"), ("s2", "1", "b", "resolved")], self.edges(bridge))

        self.append([self.source("s1", "2", "c", ingest=LATER)])
        self.build()
        self.assertEqual(
            [("s1", "1", "a", "resolved"), ("s1", "2", "c", "resolved"), ("s2", "1", "b", "resolved")],
            self.edges(bridge),
        )

        payload = self.source("s2", "1", "c")[0]
        DeltaTable.forName(self.spark, "osducatalog").update(
            "id = 's2' AND version = '1'",
            {"data": F.lit(payload), "ingestTime": F.lit(LATEST)},
        )
        self.build()
        self.assertEqual(
            [("s1", "1", "a", "resolved"), ("s1", "2", "c", "resolved"), ("s2", "1", "c", "resolved")],
            self.edges(bridge),
        )

        self.spark.sql(
            "UPDATE osducatalog SET isActive = false, ingestTime = TIMESTAMP '2026-05-01 00:00:00' "
            "WHERE id = 's1' AND version = '1'"
        )
        self.build()
        self.assertEqual([("s1", "2", "c", "resolved"), ("s2", "1", "c", "resolved")], self.edges(bridge))


class RelationshipPublicationSafetyTests(NotebookIntegrationBase):
    def test_missing_tombstone_schema_retains_watermark_until_bridge_cleanup_succeeds(self):
        target_id = "test:reference-data--FacilityType:tombstone"
        source_ids = {"test:well:tombstone-a", "test:well:tombstone-b"}
        for output_mode, missing_mode in product(("normalized", "wide"), ("skip", "infer", "fail")):
            with self.subTest(output_mode=output_mode, missing_mode=missing_mode):
                prefix = f"tombstone_{output_mode}_{missing_mode}_"
                settings = {
                    "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": prefix,
                    "WRITE_MODE": "upsert", "VERSION_STRATEGY": "merge", "OUTPUT_MODE": output_mode,
                    "INCREMENTAL_WATERMARK_MODE": "auto",
                    "INCREMENTAL_STATE_TABLE": prefix + "state", "PERSIST_SCHEMA_CACHE": False,
                    "MISSING_SCHEMA_MODE": missing_mode, "SCHEMA_PREFLIGHT": missing_mode == "skip",
                }
                write_bronze(self.spark, [
                    *(row(source_id, "1", {"FacilityName": source_id, "FacilityTypeID": target_id + ":"})
                      for source_id in sorted(source_ids)),
                    row(target_id, "1", {"Name": "Tombstone target"}, kind=FACILITY_TYPE_KIND),
                ])
                initial = run(self.spark, settings)
                self.assertEqual(["success"], [result.status for result in initial["results"]])
                parent = initial["results"][0].parent_table
                bridge = next(iter(bridge_tables(initial)))
                state = self.read(prefix + "state").collect()
                retained_bridge = self.read(bridge).collect()
                self.assertEqual(source_ids, {record.source_record_key.rsplit(":", 1)[0] for record in retained_bridge})
                for source_id, timestamp in zip(sorted(source_ids), (LATER, LATEST)):
                    self.spark.sql(
                        f"UPDATE osducatalog SET isActive = false, ingestTime = TIMESTAMP '{timestamp:%Y-%m-%d %H:%M:%S}' "
                        f"WHERE id = '{source_id}'"
                    )
                with self.assertRaisesRegex(RuntimeError, "kind group.*failed"):
                    run(self.spark, settings, missing_kinds=[WELL_KIND])
                failure = self.read("silver_run_info").where("status = 'failed'").orderBy(
                    "end_time", ascending=False,
                ).first()
                self.assertIn("inactive-source bridge cleanup", failure.error_message)
                self.assertEqual(self.read(prefix + "state").collect(), state)
                self.assertEqual(self.read(bridge).collect(), retained_bridge)
                self.assertEqual(source_ids, {record.id for record in self.read(parent).collect()})
                recovered = run(self.spark, settings)
                self.assertEqual(["success"], [result.status for result in recovered["results"]])
                self.assertEqual(self.read(bridge).count(), 0)
                self.assertEqual(self.read(parent).count(), 0)
                watermarks = [record.watermark_value for record in self.read(prefix + "state").collect()]
                self.assertCountEqual(watermarks, ["2026-01-01 00:00:00", "2026-03-01 00:00:00"])

    def test_disabled_bridges_allow_missing_schema_tombstones_without_touching_bridges(self):
        target_id = "test:reference-data--FacilityType:disabled-schema"
        source_id = "test:well:disabled-schema"
        settings = {
            "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": "disabled_schema_",
            "WRITE_MODE": "upsert", "VERSION_STRATEGY": "merge", "INCREMENTAL_WATERMARK_MODE": "auto",
            "INCREMENTAL_STATE_TABLE": "disabled_schema_state", "PERSIST_SCHEMA_CACHE": False,
            "MISSING_SCHEMA_MODE": "fail",
        }
        write_bronze(self.spark, [
            row(source_id, "1", {"FacilityName": "Source", "FacilityTypeID": target_id + ":"}),
            row(target_id, "1", {"Name": "Target"}, kind=FACILITY_TYPE_KIND),
        ])
        initial = run(self.spark, settings)
        self.assertEqual(["success"], [result.status for result in initial["results"]])
        bridge = next(iter(bridge_tables(initial)))
        retained = self.read(bridge).collect()
        self.spark.sql(
            f"UPDATE osducatalog SET isActive = false, ingestTime = TIMESTAMP '{LATER:%Y-%m-%d %H:%M:%S}' "
            f"WHERE id = '{source_id}'"
        )
        deleted = run(self.spark, {
            **settings, "WRITE_RELATIONSHIP_BRIDGES": False, "MISSING_SCHEMA_MODE": "fail",
        }, missing_kinds=[WELL_KIND])
        self.assertEqual(["success"], [result.status for result in deleted["results"]])
        self.assertEqual(self.read(initial["results"][0].parent_table).count(), 0)
        self.assertEqual(self.read(bridge).collect(), retained)
        self.assertCountEqual(
            [record.watermark_value for record in self.read("disabled_schema_state").collect()],
            ["2026-01-01 00:00:00", "2026-02-01 00:00:00"],
        )

    def test_failed_bridge_write_is_retryable_and_disabled_bridges_are_retained(self):
        target_id = "test:reference-data--FacilityType:Well"
        target_kind = "osdu:wks:reference-data--FacilityType:1.0.0"
        source_id = "test:well:bridge-source"
        settings = {
            "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": "safe_",
            "WRITE_MODE": "upsert", "VERSION_STRATEGY": "merge",
            "INCREMENTAL_WATERMARK_MODE": "auto",
        }
        write_bronze(self.spark, [
            row(source_id, "1", {"FacilityName": "Source", "FacilityTypeID": target_id + ":"}),
            row(target_id, "1", {"Name": "Well"}, kind=target_kind),
        ])
        namespace = run(self.spark, settings)
        bridge = next(iter(bridge_tables(namespace)))
        state_count = self.read("silver_incremental_state").count()
        self.spark.sql(
            f"UPDATE osducatalog SET ingestTime = TIMESTAMP '{LATER:%Y-%m-%d %H:%M:%S}' "
            f"WHERE id = '{source_id}'"
        )
        build_arguments = {
            "kinds": [WELL_KIND], "workspace_id": "fixture-workspace", "lakehouse_id": "fixture-lakehouse",
            "bronze_table": "osducatalog", "incremental": True, "table_prefix": "safe_",
            "allow_overwrite": True, "version_strategy": "merge",
            "drop_wkt": namespace["drop_wkt"],
        }
        child_write = namespace["_incremental_child_write"]

        def fail_bridge(spark, frame, target, *args, **kwargs):
            if target == bridge:
                raise OSError("Injected bridge publication failure")
            return child_write(spark, frame, target, *args, **kwargs)

        with mock.patch.dict(namespace, {"_incremental_child_write": fail_bridge}):
            with self.assertRaisesRegex(OSError, "Injected bridge"):
                namespace["run_silver_build"](self.spark, **build_arguments)
        self.assertEqual(self.read("silver_incremental_state").count(), state_count)
        self.assertEqual(
            [result.status for result in namespace["run_silver_build"](self.spark, **build_arguments)],
            ["success"],
        )
        self.assertEqual(self.read("silver_incremental_state").count(), state_count + 1)
        self.spark.sql(
            "UPDATE osducatalog SET version = '2', ingestTime = TIMESTAMP '2026-03-01 00:00:00' "
            f"WHERE id = '{source_id}'"
        )
        namespace = run(self.spark, {
            **settings, "TABLE_PREFIX": "safe_identity_", "WRITE_MODE": "full_refresh", "MERGE_KEY_COLUMNS": ["id"],
        })
        self.assertEqual(["success"], [result.status for result in namespace["results"]])
        bridge = next(iter(bridge_tables(namespace)))
        build_arguments["table_prefix"] = "safe_identity_"
        self.assertEqual({record.source_record_key.rsplit(":", 1)[1] for record in self.read(bridge).collect()}, {"2"})
        retained = self.read(bridge).collect()
        self.spark.sql(
            "UPDATE osducatalog SET isActive = false, ingestTime = TIMESTAMP '2026-04-01 00:00:00' "
            f"WHERE id = '{source_id}'"
        )
        namespace["run_silver_build"](
            self.spark, **build_arguments, merge_key_columns=["id"], write_relationship_bridges=False,
        )
        self.assertEqual(self.read("safe_identity_osdu_wks_well").count(), 0)
        self.assertEqual(self.read(bridge).collect(), retained)

    def test_payload_merge_keys_fail_before_bronze_access(self):
        from test_notebook_simplification import extract_functions, load_notebook

        build = extract_functions(load_notebook(), ["run_silver_build"])["run_silver_build"]
        read = mock.Mock(side_effect=AssertionError("No source access before validation"))
        build.__globals__.update(
            perf_counter=lambda: 0,
            uuid=SimpleNamespace(uuid4=lambda: "fixture-run"),
            datetime=datetime, UTC=timezone.utc,
            _effective_merge_key_columns=lambda value: value,
            validate_incremental_limit_safety=mock.Mock(),
            _acquire_writer=mock.Mock(), _release_writer=mock.Mock(),
            _begin_publication=read, logger=mock.Mock(),
        )
        with self.assertRaisesRegex(ValueError, "identity merge keys"):
            build(mock.Mock(), [WELL_KIND], "workspace", "lakehouse", bronze_table="osducatalog",
                  incremental=True, merge_key_columns=["custom_key"])
        read.assert_not_called()


class IncrementalConsistencyTests(NotebookIntegrationBase):
    TARGET_ID = "test:reference-data--FacilityType:dependency"
    SOURCE_ID = "test:well:source"

    def setUp(self):
        database = "consistency_" + self._testMethodName.removeprefix("test_")
        previous_database = self.spark.catalog.currentDatabase()
        self.spark.sql(f"CREATE DATABASE `{database}`")
        self.addCleanup(lambda: self.spark.sql(f"DROP DATABASE `{database}` CASCADE"))
        self.addCleanup(self.spark.catalog.setCurrentDatabase, previous_database)
        self.spark.catalog.setCurrentDatabase(database)

    def settings(self, prefix, **overrides):
        return {
            "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": prefix,
            "WRITE_MODE": "upsert", "VERSION_STRATEGY": "merge",
            "INCREMENTAL_WATERMARK_MODE": "auto", "INCREMENTAL_STATE_TABLE": prefix + "state",
            "PERSIST_SCHEMA_CACHE": False, **overrides,
        }

    def source(self, **overrides):
        return row(self.SOURCE_ID, "1", {
            "FacilityName": "Source", "FacilityTypeID": self.TARGET_ID + ":",
            "NameAliases": [{"AliasName": "Original"}],
        }, **overrides)

    def target(self, version="1", **overrides):
        return row(self.TARGET_ID, version, {"Name": "Target"}, kind=FACILITY_TYPE_KIND, **overrides)

    def bridge(self, namespace, kind=WELL_KIND):
        return next(iter(bridge_tables(namespace, kind)))

    def edges(self, table):
        return {
            (*record.source_record_key.rsplit(":", 1), record.target_record_key.rsplit(":", 1)[1], record.status)
            for record in self.read(table).collect()
        }

    def delta_version(self, table):
        return self.spark.sql(f"DESCRIBE HISTORY `{table}` LIMIT 1").first()["version"]

    def test_invalid_record_identities_fail_before_output_mutation(self):
        from pyspark.sql import functions as F

        prefix = "identity_guard_"
        settings = self.settings(prefix)
        cases = [
            ([self.source(), row(self.SOURCE_ID, "1", {}, kind=FACILITY_TYPE_KIND)], None, "Cross-kind"),
            ([row(None, "1", {})], None, "nonempty"),
            ([row(self.SOURCE_ID, None, {})], None, "nonempty"),
            ([row(self.SOURCE_ID, "1:2", {})], None, "ASCII-digit"),
            ([row(self.SOURCE_ID, "1\n", {})], None, "ASCII-digit"),
            ([row(self.SOURCE_ID, "-1", {})], None, "ASCII-digit"),
            ([self.source()], "record_key", "reserved"),
            ([self.source()], "Record-Key", "reserved"),
            ([self.source()], "missing_version", "requires"),
        ]
        for (rows, alteration, message), cap in product(cases, (0, 100)):
            with self.subTest(alteration=alteration, message=message, cap=cap):
                frame = self.spark.createDataFrame(rows, BRONZE_DDL)
                if alteration == "missing_version":
                    frame = frame.drop("version")
                elif alteration:
                    frame = frame.withColumn(alteration, F.lit("conflict"))
                frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("osducatalog")
                with self.assertRaisesRegex(ValueError, message):
                    run(self.spark, {**settings, "DATA_QUALITY_MAX_EXAMPLES": cap, "DATA_QUALITY_CHECKS": cap > 0})
                self.assertFalse(self.spark.catalog.tableExists(prefix + "osdu_wks_well"))
                self.assertFalse(self.spark.catalog.tableExists(prefix + "state"))

    def test_incompatible_bridge_contract_fails_before_parent_mutation(self):
        prefix = "bridge_contract_"
        settings = self.settings(prefix)
        write_bronze(self.spark, [self.source(), self.target()])
        namespace = run(self.spark, settings)
        bridge = self.bridge(namespace)
        parent = prefix + "osdu_wks_well"
        parent_version = self.delta_version(parent)
        self.spark.sql(f"ALTER TABLE `{bridge}` ADD COLUMNS (source_id STRING)")
        self.spark.sql("UPDATE osducatalog SET ingestTime = TIMESTAMP '2026-02-01 00:00:00'")
        for write_mode in ("upsert", "full_refresh"):
            with self.subTest(write_mode=write_mode), self.assertRaisesRegex(RuntimeError, "Incompatible relationship bridge contract"):
                run(self.spark, {**settings, "WRITE_MODE": write_mode})
            self.assertEqual(self.delta_version(parent), parent_version)

    def test_custom_source_named_keys_do_not_route_ordinary_outputs_as_bridges(self):
        from pyspark.sql import functions as F

        for key in ("source_id", "source_version", "source_kind"):
            with self.subTest(key=key):
                prefix = f"custom_{key}_"
                settings = self.settings(
                    prefix, MERGE_KEY_COLUMNS=[key], WRITE_RELATIONSHIP_BRIDGES=False,
                )
                parent = prefix + "osdu_wks_well"
                child = parent + "___namealiases"
                for version, alias, active, timestamp in (
                    ("1", "Original", True, EARLIER),
                    ("2", "Replacement", True, LATER),
                    ("2", "Replacement", False, LATEST),
                ):
                    record = row(self.SOURCE_ID, version, {
                        "FacilityName": alias, "NameAliases": [{"AliasName": alias}],
                    }, active=active, ingest=timestamp)
                    frame = self.spark.createDataFrame([record], BRONZE_DDL).withColumn(key, F.lit("custom-identity"))
                    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("osducatalog")
                    namespace = run(self.spark, settings)
                    self.assertEqual(["success"], [result.status for result in namespace["results"]])
                    self.assertFalse(bridge_tables(namespace))
                    if active:
                        self.assertEqual({(self.SOURCE_ID, version)}, keys(self.read(parent)))
                        children = self.read(child).collect()
                        self.assertEqual([(alias, "custom-identity")], [(record.AliasName, record[key]) for record in children])
                        self.assertEqual(children[0].record_key, self.read(parent).first().record_key)
                    else:
                        self.assertEqual(self.read(parent).count(), 0)
                        self.assertEqual(self.read(child).count(), 0)

    def test_target_changes_refresh_selected_sources_without_rewriting_parents(self):
        for output_mode in ("normalized", "wide"):
            with self.subTest(output_mode=output_mode):
                self.assert_target_dependency_lifecycle(output_mode)

    def assert_target_dependency_lifecycle(self, output_mode):
        prefix = f"dependency_{output_mode}_"
        parent_table = prefix + "osdu_wks_well"
        settings = self.settings(prefix, KINDS=[WELL_KIND, WELLBORE_KIND], OUTPUT_MODE=output_mode)
        missing_id = "test:reference-data--FacilityType:arriving"
        write_bronze(self.spark, [
            self.source(),
            row("test:well:pinned", "1", {"FacilityTypeID": self.TARGET_ID + ":1"}),
            row("test:well:missing", "1", {"FacilityTypeID": missing_id + ":"}),
            row("test:wellbore:unselected", "1", {"FacilityTypeID": self.TARGET_ID + ":"},
                kind=WELLBORE_KIND),
            row("test:well:boundary", "1", {}, active=False, ingest=LATER),
            self.target(),
        ])
        initial = run(self.spark, settings)
        bridge = self.bridge(initial)
        unselected_bridge = self.bridge(initial, WELLBORE_KIND)
        unselected_rows = self.read(unselected_bridge).collect()
        parent_version = self.delta_version(parent_table)
        parent_rows = self.read(parent_table).collect()
        child_table = parent_table + "___namealiases"
        child_version = self.delta_version(child_table) if output_mode == "normalized" else None
        self.spark.sql(
            "UPDATE osducatalog SET isActive = false, ingestTime = TIMESTAMP '2026-03-01 00:00:00' "
            f"WHERE id = '{self.TARGET_ID}'"
        )
        write_bronze(self.spark, [
            self.target("2", ingest=LATEST),
            row(missing_id, "1", {"Name": "Arrived"}, kind=FACILITY_TYPE_KIND, ingest=LATEST),
        ], mode="append")
        preview = run(self.spark, {**settings, "RUN_PROFILE": "dry_run", "KINDS": [WELL_KIND]})
        planned = preview["dry_run_results"][0]
        self.assertTrue(planned["dependency_maintenance"])
        self.assertTrue(planned["would_write"])
        self.assertIn(bridge, planned["relationship_bridge_tables"])
        self.assertEqual(parent_version, self.delta_version(parent_table))
        run(self.spark, {**settings, "KINDS": [WELL_KIND]})
        self.assertEqual({
            (self.SOURCE_ID, "1", "2", "resolved"),
            ("test:well:pinned", "1", "1", "target_deleted"),
            ("test:well:missing", "1", "1", "resolved"),
        }, self.edges(bridge))
        self.assertEqual(parent_version, self.delta_version(parent_table))
        if child_version is not None:
            self.assertEqual(child_version, self.delta_version(child_table))
        self.assertCountEqual(parent_rows, self.read(parent_table).collect())
        self.assertEqual(unselected_rows, self.read(unselected_bridge).collect())
        self.spark.sql(
            "UPDATE osducatalog SET isActive = (version = '1') "
            f"WHERE id = '{self.TARGET_ID}'"
        )
        run(self.spark, {**settings, "EXCLUDED_KINDS": [WELLBORE_KIND, FACILITY_TYPE_KIND]})
        self.assertEqual({
            (self.SOURCE_ID, "1", "2", "target_deleted"),
            ("test:well:pinned", "1", "1", "resolved"),
            ("test:well:missing", "1", "1", "resolved"),
        }, self.edges(bridge))
        self.assertEqual(parent_version, self.delta_version(parent_table))
        self.assertEqual(unselected_rows, self.read(unselected_bridge).collect())
        oracle = run(self.spark, {**settings, "KINDS": [WELL_KIND],
                                 "TABLE_PREFIX": prefix + "oracle_", "WRITE_MODE": "full_refresh"})
        self.assertEqual(self.edges(bridge), self.edges(self.bridge(oracle)))

    def test_cleared_arrays_remove_only_affected_parent_children(self):
        for label, replacement in (("absent", {}), ("null", {"NameAliases": None}),
                                   ("empty", {"NameAliases": []})):
            with self.subTest(representation=label):
                prefix = f"clear_{label}_"
                settings = self.settings(prefix, CREATE_EMPTY_CHILD_TABLES=False)
                write_bronze(self.spark, [
                    self.source(),
                    row("test:well:retained", "1", {"NameAliases": [{"AliasName": "Retained"}]}),
                ])
                run(self.spark, settings)
                payload = row(self.SOURCE_ID, "1", {"FacilityName": "Cleared", **replacement})[0]
                self.spark.sql(
                    f"UPDATE osducatalog SET data = '{payload}', "
                    f"ingestTime = TIMESTAMP '2026-02-01 00:00:00' WHERE id = '{self.SOURCE_ID}'"
                )
                run(self.spark, settings)
                children = self.read(prefix + "osdu_wks_well___namealiases").collect()
                self.assertEqual([("test:well:retained", "Retained")],
                                 [(record.id, record.AliasName) for record in children])
                self.assertEqual({(self.SOURCE_ID, "1"), ("test:well:retained", "1")},
                                 keys(self.read(prefix + "osdu_wks_well")))

    def test_id_only_live_replacement_survives_older_tombstone_in_every_output(self):
        for watermark_mode in ("auto", "off"):
            with self.subTest(watermark_mode=watermark_mode):
                self.assert_live_replacement_survives(watermark_mode)

    def assert_live_replacement_survives(self, watermark_mode):
        prefix = f"replacement_{watermark_mode}_"
        settings = self.settings(prefix, MERGE_KEY_COLUMNS=["id"], INCREMENTAL_WATERMARK_MODE=watermark_mode)
        write_bronze(self.spark, [self.source(), self.target()])
        initial = run(self.spark, settings)
        bridge = self.bridge(initial)
        self.spark.sql(
            "UPDATE osducatalog SET isActive = false, ingestTime = TIMESTAMP '2026-02-01 00:00:00' "
            f"WHERE id = '{self.SOURCE_ID}'"
        )
        write_bronze(self.spark, [
            row(self.SOURCE_ID, "2", {"FacilityName": "Replacement", "FacilityTypeID": self.TARGET_ID + ":",
                                     "NameAliases": [{"AliasName": "Replacement"}]}, ingest=LATER),
        ], mode="append")
        run(self.spark, settings)
        self.assertEqual({(self.SOURCE_ID, "2")}, keys(self.read(prefix + "osdu_wks_well")))
        children = self.read(prefix + "osdu_wks_well___namealiases").collect()
        self.assertEqual([(self.SOURCE_ID, "Replacement")], [(record.id, record.AliasName) for record in children])
        self.assertEqual({(self.SOURCE_ID, "2")}, {tuple(record.record_key.rsplit(":", 1)) for record in children})
        self.assertEqual(self.read(prefix + "osdu_wks_well").first().record_key, children[0].record_key)
        self.assertEqual({(self.SOURCE_ID, "2", "1", "resolved")}, self.edges(bridge))

    def test_prefix_bootstrap_does_not_borrow_another_destinations_checkpoint(self):
        settings = self.settings("owner_a_")
        write_bronze(self.spark, [
            self.source(), row("test:well:boundary", "1", {"FacilityName": "Boundary"}, ingest=LATER),
            self.target(),
        ])
        initial = run(self.spark, settings)
        retained = self.read("owner_a_osdu_wks_well").collect()
        destination = run(self.spark, {**settings, "TABLE_PREFIX": "owner_b_"})
        self.assertEqual(keys(self.read("owner_a_osdu_wks_well")), keys(self.read("owner_b_osdu_wks_well")))
        self.assertEqual(self.edges(self.bridge(initial)), self.edges(self.bridge(destination)))
        self.assertCountEqual(retained, self.read("owner_a_osdu_wks_well").collect())

    def test_incompatible_contract_rejects_before_existing_outputs_change(self):
        settings = self.settings("contract_")
        write_bronze(self.spark, [self.source(), self.target()])
        initial = run(self.spark, settings)
        owned = ["contract_osdu_wks_well", "contract_osdu_wks_well___namealiases", self.bridge(initial)]
        versions = {table: self.delta_version(table) for table in owned}
        state = self.read("contract_state").collect()
        self.spark.sql("CREATE TABLE contract_bronze USING DELTA AS SELECT * FROM osducatalog")
        for change in (
            {"OUTPUT_MODE": "wide"}, {"MERGE_KEY_COLUMNS": ["id"]},
            {"VERSION_STRATEGY": "versioned_tables"}, {"INCLUDE_INACTIVE_RECORDS": True},
            {"BRONZE_TABLE": "contract_bronze"},
            {"INCREMENTAL_STATE_TABLE": "contract_other_state"},
            {"RUN_MANIFEST_TABLE": "contract_other_manifest"},
            {"ADME_ENDPOINT": "https://other.example.test"}, {"ADME_DATA_PARTITION_ID": "other-partition"},
            {"CREATE_EMPTY_CHILD_TABLES": False},
        ):
            with self.subTest(change=change):
                with self.assertRaisesRegex((ValueError, RuntimeError), "(?i)rebuild|full.refresh|migration"):
                    run(self.spark, {**settings, **change})
                self.assertEqual(versions, {table: self.delta_version(table) for table in owned})
                self.assertEqual(state, self.read("contract_state").collect())
        timezone = self.spark.conf.get("spark.sql.session.timeZone")
        try:
            with self.subTest(session_timezone="America/Chicago"):
                with mock.patch.dict(os.environ, {"ADME_SPARK_TIMEZONE": "America/Chicago"}):
                    with self.assertRaisesRegex((ValueError, RuntimeError), "(?i)rebuild|full.refresh|migration"):
                        run(self.spark, settings)
                self.assertEqual(versions, {table: self.delta_version(table) for table in owned})
                self.assertEqual(state, self.read("contract_state").collect())
        finally:
            self.spark.conf.set("spark.sql.session.timeZone", timezone)

    def test_initial_empty_normalized_refresh_is_repeatable_but_rejects_materialized_orphans(self):
        self.assert_initial_empty_refresh("normalized")

    def test_initial_empty_wide_refresh_is_repeatable_but_rejects_materialized_orphans(self):
        self.assert_initial_empty_refresh("wide")

    def assert_initial_empty_refresh(self, output_mode):
        write_bronze(self.spark, [self.target()])
        prefix = f"empty_initial_{output_mode}_"
        settings = self.settings(prefix, WRITE_MODE="full_refresh", OUTPUT_MODE=output_mode)
        parent = prefix + "osdu_wks_well"
        for _ in range(2):
            namespace = run(self.spark, settings)
            self.assertEqual(["success"], [result.status for result in namespace["results"]])
            self.assertEqual(["empty_refresh"], [result.schema_mode for result in namespace["results"]])
            self.assertFalse(self.spark.catalog.tableExists(parent))
            bridges = bridge_tables(namespace)
            self.assertTrue(bridges)
            self.assertTrue(all(not self.spark.catalog.tableExists(table) for table in bridges))
        attempts = self.read("silver_publication_attempts").select("status").collect()
        self.assertEqual(["committed", "committed"], [record.status for record in attempts])
        bridge = next(iter(bridges))
        retained = [(self.SOURCE_ID + ":1", self.TARGET_ID + ":1", WELL_KIND, "resolved", "fixture-run")]
        self.spark.createDataFrame(
            retained,
            "source_record_key string, target_record_key string, source_kind string, status string, run_id string",
        ).write.format("delta").saveAsTable(bridge)
        version = self.delta_version(bridge)
        with self.assertRaisesRegex(RuntimeError, "have no parent table"):
            run(self.spark, settings)
        self.assertFalse(self.spark.catalog.tableExists(parent))
        self.assertEqual(version, self.delta_version(bridge))
        self.assertEqual(retained, [tuple(record) for record in self.read(bridge).collect()])

    def test_empty_full_refresh_reconciles_owned_outputs_without_erasing_unselected_kind(self):
        for selector in ([WELL_KIND], ["osdu:wks:master-data--Well:1.*"]):
            for shared_family in (False, True):
                with self.subTest(selector=selector, shared_family=shared_family):
                    self.assert_empty_selected_refresh(selector, shared_family)

    def assert_empty_selected_refresh(self, selector, shared_family):
        prefix = ("empty_exact_" if selector == [WELL_KIND] else "empty_wildcard_") + f"{int(shared_family)}_"
        revision = WELL_KIND.rsplit(":", 1)[0] + ":2.0.0"
        selected = [WELL_KIND, WELLBORE_KIND] + ([revision] if shared_family else [])
        settings = self.settings(prefix, WRITE_MODE="full_refresh", KINDS=selected)
        retained_source = "test:well:other-schema"
        siblings = [
            row(retained_source, "3", {"FacilityName": "Retained schema", "FacilityTypeID": self.TARGET_ID + ":",
                                       "NameAliases": [{"AliasName": "Retained schema"}]}, kind=revision),
        ] if shared_family else []
        write_bronze(self.spark, [
            self.source(), self.target(),
            row("test:wellbore:retained", "1", {"FacilityTypeID": self.TARGET_ID + ":"}, kind=WELLBORE_KIND),
            *siblings,
        ])
        initial = run(self.spark, settings)
        bridge = self.bridge(initial)
        retained_bridge = self.bridge(initial, WELLBORE_KIND)
        retained = self.read(retained_bridge).collect()
        self.spark.sql(f"DELETE FROM osducatalog WHERE kind = '{WELL_KIND}'")
        versions = {
            table: self.delta_version(table)
            for table in (prefix + "osdu_wks_well", prefix + "osdu_wks_well___namealiases", bridge)
        }
        preview = run(self.spark, {**settings, "KINDS": selector, "RUN_PROFILE": "dry_run"})
        planned = preview["dry_run_results"][0]
        self.assertEqual(WELL_KIND, planned["kind"])
        self.assertTrue(planned["empty_refresh"])
        self.assertTrue(planned["would_write"])
        self.assertIn(bridge, planned["historical_output_tables"])
        self.assertEqual(versions, {table: self.delta_version(table) for table in versions})
        with self.assertRaisesRegex((ValueError, RuntimeError), "(?i)overwrite"):
            run(self.spark, {**settings, "KINDS": selector, "ALLOW_OVERWRITE": False})
        run(self.spark, {**settings, "KINDS": selector})
        self.assertEqual(
            {(retained_source, "3")} if shared_family else set(), keys(self.read(prefix + "osdu_wks_well")),
        )
        children = self.read(prefix + "osdu_wks_well___namealiases").collect()
        self.assertEqual(
            [(retained_source, "Retained schema")] if shared_family else [],
            [(record.id, record.AliasName) for record in children],
        )
        self.assertEqual(
            {(retained_source, "3", "1", "resolved")} if shared_family else set(), self.edges(bridge),
        )
        self.assertEqual({("test:wellbore:retained", "1")}, keys(self.read(prefix + "osdu_wks_wellbore")))
        self.assertEqual(retained, self.read(retained_bridge).collect())

    def test_retired_schema_outputs_follow_effective_identity_version_retention(self):
        revision = WELL_KIND.rsplit(":", 1)[0] + ":2.0.0"
        reduced = copy.deepcopy(WELL_SCHEMA)
        properties = reduced["properties"]["data"]["allOf"][0]["properties"]
        properties.pop("FacilityTypeID")
        properties.pop("NameAliases")

        def schemas(namespace):
            namespace["_adme_schema_get_json"] = lambda url, *args, **kwargs: (
                [] if "latestVersion" in url else {"schema": reduced if url.endswith(revision) else WELL_SCHEMA}
            )

        for watermark_mode in ("auto", "off"):
            for merge_keys in (["id"], ["id", "version"], ["id", "version", "kind"]):
                with self.subTest(merge_keys=merge_keys, watermark_mode=watermark_mode):
                    prefix = f"retire_{'_'.join(merge_keys)}_{watermark_mode}_"
                    settings = self.settings(
                        prefix, MERGE_KEY_COLUMNS=merge_keys, KINDS=[WELL_KIND, revision],
                        INCREMENTAL_WATERMARK_MODE=watermark_mode,
                    )
                    write_bronze(self.spark, [self.source(), self.target()])
                    initial = run(self.spark, settings, configure=schemas)
                    bridge = self.bridge(initial)
                    retain_history = "version" in merge_keys
                    if not retain_history:
                        self.spark.sql(f"DELETE FROM osducatalog WHERE id = '{self.SOURCE_ID}'")
                    write_bronze(self.spark, [
                        row(self.SOURCE_ID, "2", {"FacilityName": "Revision"}, kind=revision, ingest=LATER),
                    ], mode="append")
                    run(self.spark, settings, configure=schemas)
                    expected = {(self.SOURCE_ID, "2")} | ({(self.SOURCE_ID, "1")} if retain_history else set())
                    self.assertEqual(expected, keys(self.read(prefix + "osdu_wks_well")))
                    self.assertEqual(int(retain_history), self.read(prefix + "osdu_wks_well___namealiases").count())
                    self.assertEqual(
                        {(self.SOURCE_ID, "1", "1", "resolved")} if retain_history else set(), self.edges(bridge),
                    )

    def test_failed_finalization_replays_retained_snapshot_before_newer_input(self):
        for failure_stage in ("parent", "child_delete", "child_append", "bridge", "metadata", "supersede"):
            with self.subTest(failure_stage=failure_stage):
                self.assert_publication_failure_recovery(failure_stage)

    def assert_publication_failure_recovery(self, failure_stage):
        prefix = f"retry_{failure_stage}_"
        parent_table = prefix + "osdu_wks_well"
        child_table = parent_table + "___namealiases"
        revision = WELL_KIND.rsplit(":", 1)[0] + ":2.0.0"
        settings = self.settings(prefix, KINDS=["osdu:wks:master-data--Well:*"])
        write_bronze(self.spark, [self.source(), self.target()])
        initial = run(self.spark, settings)
        bridge = self.bridge(initial)
        state = self.read(prefix + "state").collect()
        payload = row(self.SOURCE_ID, "1", {
            "FacilityName": "Attempt", "NameAliases": [{"AliasName": "Attempt"}],
        })[0]
        self.spark.sql(
            f"UPDATE osducatalog SET data = '{payload}', ingestTime = TIMESTAMP '2026-02-01 00:00:00' "
            f"WHERE id = '{self.SOURCE_ID}'"
        )
        injected = []

        def inject_failure(namespace):
            merge = namespace["_execute_delta_merge"]
            write = namespace["_write_table"]

            def fail():
                injected.append(failure_stage)
                raise OSError(f"Injected {failure_stage} failure")

            def merge_then_fail(delta_table, target_df, source_df, target, *args, **kwargs):
                merge(delta_table, target_df, source_df, target, *args, **kwargs)
                if target == parent_table:
                    fail()

            def write_with_failure(frame, target, mode="overwrite", **kwargs):
                if failure_stage == "child_delete" and target == child_table and mode == "append":
                    fail()
                write(frame, target, mode=mode, **kwargs)
                if mode == "append" and (
                    (failure_stage == "child_append" and target == child_table)
                    or (failure_stage == "bridge" and target == bridge)
                ):
                    fail()

            if failure_stage == "parent":
                namespace["_execute_delta_merge"] = merge_then_fail
            elif failure_stage in {"child_delete", "child_append", "bridge"}:
                namespace["_write_table"] = write_with_failure
            else:
                namespace["flush_metadata_buffers"] = lambda *args, **kwargs: fail()

        with self.assertRaisesRegex((RuntimeError, OSError), "(?i)Injected|finalization|kind group.*failed"):
            run(self.spark, settings, configure=inject_failure)
        self.assertTrue(injected, "The requested mutation boundary was not exercised")
        self.assertEqual(state, self.read(prefix + "state").collect())
        pending = self.read(initial["_PUBLICATION_ATTEMPTS_TABLE"]).where("status = 'started'").collect()
        self.assertEqual(1, len(pending))
        recorded_ownership = {
            record.table_name for record in self.read(initial["_OUTPUT_OWNERSHIP_TABLE"])
            .where(f"run_id = '{pending[0].run_id}'").collect()
        }
        self.assertTrue({parent_table, child_table, bridge}.issubset(recorded_ownership))
        self.assertEqual("Attempt", self.read(parent_table).first().data__FacilityName)
        if failure_stage == "child_delete":
            self.assertEqual(0, self.read(child_table).count())
        write_bronze(self.spark, [
            row("test:well:later-arrival", "1", {"FacilityName": "Later schema"}, kind=revision, ingest=LATEST),
        ], mode="append")
        version = self.delta_version(parent_table)
        for change in ({"KINDS": [WELLBORE_KIND]}, {"WRITE_RELATIONSHIP_BRIDGES": False}):
            with self.subTest(incompatible_retry=change):
                with self.assertRaisesRegex(RuntimeError, "(?i)original|incomplete|retry"):
                    run(self.spark, {**settings, **change})
                self.assertEqual(version, self.delta_version(parent_table))
        if failure_stage == "supersede":
            for overrides in ({"ALLOW_OVERWRITE": False}, {"LIMIT": 1}):
                with self.subTest(unauthorized_rebuild=overrides):
                    with self.assertRaisesRegex((RuntimeError, ValueError), "(?i)original|incomplete|retry|limit|overwrite"):
                        run(self.spark, {**settings, "WRITE_MODE": "full_refresh", **overrides})
                    self.assertEqual(version, self.delta_version(parent_table))
            rebuilt = run(self.spark, {**settings, "WRITE_MODE": "full_refresh"})
            self.assertEqual(
                {(self.SOURCE_ID, "1"), ("test:well:later-arrival", "1")}, keys(self.read(parent_table)),
            )
            abandoned = self.read(rebuilt["_PUBLICATION_ATTEMPTS_TABLE"]).where(
                f"run_id = '{pending[0].run_id}'"
            ).first()
            self.assertEqual("abandoned", abandoned.status)
            self.assertEqual(0, self.read(rebuilt["_PUBLICATION_ATTEMPTS_TABLE"]).where("status = 'started'").count())
            return
        recovered = run(self.spark, settings)
        self.assertEqual({(self.SOURCE_ID, "1")}, keys(self.read(parent_table)))
        self.assertEqual("Attempt", self.read(parent_table).first().data__FacilityName)
        self.assertEqual(["Attempt"], [record.AliasName for record in self.read(child_table).collect()])
        self.assertEqual(set(), self.edges(bridge))
        self.assertEqual(
            {"2026-01-01 00:00:00", "2026-02-01 00:00:00"},
            {record.watermark_value for record in self.read(prefix + "state").collect()},
        )
        self.assertEqual(["success"], [result.status for result in recovered["results"]])
        run(self.spark, settings)
        self.assertEqual({(self.SOURCE_ID, "1"), ("test:well:later-arrival", "1")},
                         keys(self.read(parent_table)))

    def test_reenabling_bridges_rescans_older_sources_without_parent_rewrites(self):
        settings = self.settings("reenable_", WRITE_RELATIONSHIP_BRIDGES=False)
        write_bronze(self.spark, [
            self.source(), self.target(), row("test:well:boundary", "1", {}, active=False, ingest=LATER),
        ])
        run(self.spark, settings)
        version = self.delta_version("reenable_osdu_wks_well")
        enabled = run(self.spark, {**settings, "WRITE_RELATIONSHIP_BRIDGES": True})
        self.assertEqual({(self.SOURCE_ID, "1", "1", "resolved")}, self.edges(self.bridge(enabled)))
        self.assertEqual(version, self.delta_version("reenable_osdu_wks_well"))

    def test_schema_available_after_inference_reprojects_older_successful_rows(self):
        settings = self.settings("inference_retry_", MISSING_SCHEMA_MODE="infer")
        write_bronze(self.spark, [
            self.source(), self.target(), row("test:well:boundary", "1", {}, ingest=LATER),
        ])
        inferred = run(self.spark, settings, missing_kinds=[WELL_KIND])
        self.assertEqual(["success"], [result.status for result in inferred["results"]])
        available = run(self.spark, settings)
        self.assertEqual({(self.SOURCE_ID, "1", "1", "resolved")}, self.edges(self.bridge(available)))
        self.assertEqual({(self.SOURCE_ID, "1"), ("test:well:boundary", "1")},
                         keys(self.read("inference_retry_osdu_wks_well")))
        self.assertEqual("Original", self.read("inference_retry_osdu_wks_well___namealiases").first().AliasName)

    def test_null_or_invalid_selected_watermarks_fail_before_output_mutation(self):
        for label, value in (("null", None), ("invalid", "not-a-timestamp")):
            with self.subTest(value=label):
                prefix = f"timestamp_{label}_"
                settings = self.settings(prefix)
                write_bronze(self.spark, [self.source(), self.target()])
                if value is not None:
                    self.spark.sql(
                        "CREATE OR REPLACE TABLE invalid_timestamp_bronze USING DELTA AS SELECT * FROM osducatalog"
                    )
                    settings["BRONZE_TABLE"] = "invalid_timestamp_bronze"
                run(self.spark, settings)
                version = self.delta_version(prefix + "osdu_wks_well")
                state = self.read(prefix + "state").collect()
                if value is None:
                    self.spark.sql(f"UPDATE osducatalog SET ingestTime = NULL WHERE id = '{self.SOURCE_ID}'")
                else:
                    self.spark.sql(
                        "CREATE OR REPLACE TABLE invalid_timestamp_bronze USING DELTA AS "
                        "SELECT * EXCEPT (ingestTime), "
                        f"CASE WHEN id = '{self.SOURCE_ID}' THEN '{value}' "
                        "ELSE CAST(ingestTime AS STRING) END AS ingestTime FROM osducatalog"
                    )
                with self.assertRaisesRegex((ValueError, RuntimeError), "(?i)watermark|ingestTime|timestamp"):
                    run(self.spark, settings)
                self.assertEqual(version, self.delta_version(prefix + "osdu_wks_well"))
                self.assertEqual(state, self.read(prefix + "state").collect())

    def test_older_arrivals_and_physical_deletes_require_authorized_full_refresh(self):
        settings = self.settings("reconcile_")
        write_bronze(self.spark, [
            self.source(), row("test:well:boundary", "1", {"FacilityName": "Boundary"}, ingest=LATER),
        ])
        run(self.spark, settings)
        self.spark.sql(f"DELETE FROM osducatalog WHERE id = '{self.SOURCE_ID}'")
        write_bronze(self.spark, [
            row("test:well:older", "1", {"FacilityName": "Older arrival"}, ingest=EARLIER),
            row("test:well:equal", "1", {"FacilityName": "Equal arrival"}, ingest=LATER),
        ], mode="append")
        run(self.spark, settings)
        self.assertEqual(
            {(self.SOURCE_ID, "1"), ("test:well:boundary", "1"), ("test:well:equal", "1")},
            keys(self.read("reconcile_osdu_wks_well")),
        )
        with self.assertRaisesRegex((ValueError, RuntimeError), "(?i)overwrite"):
            run(self.spark, {**settings, "WRITE_MODE": "full_refresh", "ALLOW_OVERWRITE": False})
        run(self.spark, {**settings, "WRITE_MODE": "full_refresh"})
        self.assertEqual(
            {("test:well:older", "1"), ("test:well:boundary", "1"), ("test:well:equal", "1")},
            keys(self.read("reconcile_osdu_wks_well")),
        )
        self.assertEqual(0, self.read("reconcile_osdu_wks_well___namealiases").count())

    def test_destination_guard_rejects_other_prefixes_and_wrong_owner_release(self):
        settings = self.settings("guard_owner_")
        write_bronze(self.spark, [self.source(), self.target()])
        namespace = run(self.spark, settings)
        with mock.patch.dict(namespace, {"_table_path_uri": lambda table: Path(self.temporary.name).as_uri()}):
            with self.assertRaisesRegex(RuntimeError, "refusing initialization"):
                namespace["_table_exists"](self.spark, "existing_non_delta_directory", refresh=True)
        self.assertNotIn("existing_non_delta_directory", namespace["_TABLE_EXISTS_CACHE"])
        owner = "synthetic-paused-writer"
        namespace["_acquire_writer"](self.spark, owner)
        try:
            namespace["_release_writer"](self.spark, "synthetic-wrong-owner")
            version = self.delta_version("guard_owner_osdu_wks_well")
            for change in ({}, {"TABLE_PREFIX": "guard_other_", "KINDS": [WELLBORE_KIND]},
                           {"WRITE_MODE": "full_refresh"}):
                with self.subTest(contender=change):
                    with self.assertRaisesRegex(RuntimeError, "(?i)writer|ownership"):
                        run(self.spark, {**settings, **change})
                    self.assertEqual(version, self.delta_version("guard_owner_osdu_wks_well"))
                    self.assertFalse(self.spark.catalog.tableExists("guard_other_osdu_wks_wellbore"))
        finally:
            namespace["_release_writer"](self.spark, owner)
        resumed = run(self.spark, settings)
        self.assertEqual(["success"], [result.status for result in resumed["results"]])

    def test_source_and_relationships_use_one_snapshot_when_bronze_changes_after_capture(self):
        settings = self.settings("pinned_input_")
        write_bronze(self.spark, [self.source(), self.target()])
        run(self.spark, settings)
        captured = []

        def change_after_capture(namespace):
            begin = namespace["_begin_publication"]

            def begin_then_append(*args, **kwargs):
                publication = begin(*args, **kwargs)
                captured.append(publication["source_version"])
                write_bronze(self.spark, [
                    self.target("2", ingest=LATER),
                    row("test:well:after-capture", "1", {"FacilityTypeID": self.TARGET_ID + ":"}, ingest=LATER),
                ], mode="append")
                return publication

            namespace["_begin_publication"] = begin_then_append

        pinned = run(self.spark, settings, configure=change_after_capture)
        self.assertEqual(1, len(captured))
        self.assertEqual({(self.SOURCE_ID, "1")}, keys(self.read("pinned_input_osdu_wks_well")))
        self.assertEqual({(self.SOURCE_ID, "1", "1", "resolved")}, self.edges(self.bridge(pinned)))
        following = run(self.spark, settings)
        self.assertEqual(
            {(self.SOURCE_ID, "1"), ("test:well:after-capture", "1")},
            keys(self.read("pinned_input_osdu_wks_well")),
        )
        self.assertEqual({
            (self.SOURCE_ID, "1", "2", "resolved"), ("test:well:after-capture", "1", "2", "resolved"),
        }, self.edges(self.bridge(following)))

    def test_final_status_failure_does_not_make_prepared_checkpoints_eligible(self):
        for failure_stage in ("status", "documentation", "quality", "checkpoint", "attempt_commit", "attempt_ack"):
            with self.subTest(failure_stage=failure_stage):
                self.assert_checkpoint_eligibility_after_failure(failure_stage)

    def assert_checkpoint_eligibility_after_failure(self, failure_stage):
        prefix = f"finalize_{failure_stage}_"
        settings = self.settings(prefix, METADATA_FLUSH_INTERVAL=1)
        write_bronze(self.spark, [self.source(), self.target()])
        initial = run(self.spark, settings)
        context_id = self.read(prefix + "state").first()["context_id"]
        self.spark.sql(
            "UPDATE osducatalog SET ingestTime = TIMESTAMP '2026-02-01 00:00:00' "
            f"WHERE id = '{self.SOURCE_ID}'"
        )

        def fail_status(namespace):
            write_status = namespace["write_run_status"]
            write = namespace["_write_table"]
            delta_target = namespace["_delta_table_for_target"]

            def write_before_commit(spark, run_id, status, *args, **kwargs):
                if status == "committed":
                    raise OSError("Injected final status failure")
                return write_status(spark, run_id, status, *args, **kwargs)

            def write_checkpoint_then_fail(frame, target, *args, **kwargs):
                write(frame, target, *args, **kwargs)
                if target == prefix + "state":
                    raise OSError("Injected final checkpoint failure")

            def block_attempt_commit(spark, target):
                table = delta_target(spark, target)
                if target == namespace["_PUBLICATION_ATTEMPTS_TABLE"]:
                    def update_then_fail(*args, **kwargs):
                        table.update(*args, **kwargs)
                        raise OSError("Injected final attempt acknowledgement failure")

                    return mock.Mock(
                        wraps=table, update=mock.Mock(
                            side_effect=update_then_fail if failure_stage == "attempt_ack"
                            else OSError("Injected final attempt commit failure"),
                        ),
                    )
                return table

            if failure_stage == "status":
                namespace["write_run_status"] = write_before_commit
            elif failure_stage == "checkpoint":
                namespace["_write_table"] = write_checkpoint_then_fail
            elif failure_stage in {"documentation", "quality"}:
                helper = (
                    "flush_output_documentation_rows" if failure_stage == "documentation"
                    else "flush_data_quality_issue_rows"
                )
                flush = namespace[helper]

                def flush_then_fail(*args, **kwargs):
                    flush(*args, **kwargs)
                    raise OSError(f"Injected final {failure_stage} failure")

                namespace[helper] = flush_then_fail
            else:
                namespace["_delta_table_for_target"] = block_attempt_commit

        with self.assertRaisesRegex((OSError, RuntimeError), "(?i)final.*status|finalization|Injected"):
            run(self.spark, settings, configure=fail_status)
        self.assertEqual(
            {WELL_KIND: "2026-02-01 00:00:00" if failure_stage == "attempt_ack" else "2026-01-01 00:00:00"},
            initial["load_incremental_watermark_state"](
                self.spark, [WELL_KIND], "ingestTime", context_id=context_id,
            ),
        )
        recovered = run(self.spark, settings)
        self.assertEqual(
            {WELL_KIND: "2026-02-01 00:00:00"},
            recovered["load_incremental_watermark_state"](
                self.spark, [WELL_KIND], "ingestTime", context_id=context_id,
            ),
        )


class SchemaInferenceTests(NotebookIntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [
            row("test:wellbore:1", "1", {"FacilityName": "Bore", "Depth": 12.5}, kind=WELLBORE_KIND),
            row("test:wellbore:2", "1", {"FacilityName": "Bore 2", "Depth": 7.0}, kind=WELLBORE_KIND),
        ])
        base = {"RUN_PROFILE": "execute", "KINDS": [WELLBORE_KIND], "WRITE_MODE": "full_refresh",
                "VERSION_STRATEGY": "merge"}
        cls.inferred = run(cls.spark, {**base, "TABLE_PREFIX": "inf_", "MISSING_SCHEMA_MODE": "infer"},
                           missing_kinds=[WELLBORE_KIND])
        cls.skipped = run(cls.spark, {**base, "TABLE_PREFIX": "skp_", "MISSING_SCHEMA_MODE": "skip"},
                          missing_kinds=[WELLBORE_KIND])

    def test_infer_mode_publishes_rows_with_inferred_column_types(self):
        table = self.read("inf_osdu_wks_wellbore")
        self.assertEqual(2, table.count())
        self.assertEqual("double", dict(table.dtypes)["data__Depth"])

    def test_skip_mode_reports_the_kind_as_schema_missing(self):
        self.assertEqual(["schema_missing"], [r.status for r in self.skipped["results"]])
        self.assertFalse(self.spark.catalog.tableExists("skp_osdu_wks_wellbore"))


class DryRunTests(NotebookIntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [row("test:well:1", "1", {"FacilityName": "Alpha"})])
        cls.namespace = run(cls.spark, {
            "RUN_PROFILE": "dry_run", "KINDS": [WELL_KIND], "TABLE_PREFIX": "dry_", "WRITE_MODE": "full_refresh",
            "VERSION_STRATEGY": "merge",
        })

    def test_dry_run_creates_no_output_tables(self):
        for strategy, expected in (
            ("merge", "dry_osdu_wks_well"),
            ("versioned_tables", "dry_osdu_wks_well__v1_0_0"),
        ):
            with self.subTest(strategy=strategy):
                preview = run(self.spark, {
                    "RUN_PROFILE": "dry_run", "KINDS": [WELL_KIND], "TABLE_PREFIX": "dry_",
                    "WRITE_MODE": "full_refresh", "VERSION_STRATEGY": strategy,
                })
                self.assertEqual(expected, preview["dry_run_results"][0]["parent_table"])
        names = {t.name for t in self.spark.catalog.listTables()}
        self.assertFalse([n for n in names if n.startswith("dry_")], names)


class UnresolvedSchemaFieldTests(DryRunTests):
    def test_columns_absent_from_the_schema_are_reported_as_quality_issues(self):
        registry = self.namespace["SchemaRegistry"].from_dict({WELL_KIND: WELL_SCHEMA})
        frame = self.spark.createDataFrame([("w1", "Alpha", "x")], "id string, FacilityName string, Surprise string")
        issues = self.namespace["_unresolved_schema_field_frames"](
            self.spark, "run", WELL_KIND, frame, registry,
        )
        self.assertEqual(
            ["Surprise"], [issue.collect()[0]["column_name"] for issue in issues],
        )
        self.assertEqual([], self.namespace["_unresolved_schema_field_frames"](
            self.spark, "run", "unknown:kind:1.0.0", frame, registry,
        ))
        self.assertFalse(registry.has_field("unknown:kind:1.0.0", "id"))


class AuthenticationAndContextTests(NotebookIntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [row("test:well:1", "1", {"FacilityName": "Alpha"})])
        cls.original = {}
        cls.namespace = run(cls.spark, {
            "RUN_PROFILE": "inspect", "KINDS": [WELL_KIND], "TABLE_PREFIX": "auth_", "WRITE_MODE": "full_refresh",
            "VERSION_STRATEGY": "merge",
        }, capture=cls.original)

    def patched(self, **values):
        lowered = {name.lower(): value for name, value in values.items()
                   if name.lower() == name.upper().lower() and name.lower() in self.namespace and name.isupper()}
        return mock.patch.dict(self.namespace, {**values, **lowered})

    @staticmethod
    def token(value, expires_on=4102444800):
        return SimpleNamespace(token=value, expires_on=expires_on)

    def test_managed_identity_token_is_acquired_and_cached(self):
        calls = []
        credential = SimpleNamespace(get_token=lambda scope: calls.append(scope) or self.token("mi-token"))
        self.namespace["_ADME_AUTH_STATE"]["tokens"].clear()
        with self.patched(ADME_AUTH_METHOD="MI", ManagedIdentityCredential=lambda: credential):
            self.assertEqual("mi-token", self.original["get_adme_access_token"]())
            self.assertEqual("mi-token", self.original["get_adme_access_token"]())
        self.assertEqual([self.namespace["ADME_TOKEN_SCOPE"]], calls)

    def test_managed_identity_failure_propagates_without_caching(self):
        credential = mock.Mock()
        credential.get_token.side_effect = RuntimeError("no login")
        self.namespace["_ADME_AUTH_STATE"]["tokens"].clear()
        with self.patched(ADME_AUTH_METHOD="MI", ManagedIdentityCredential=lambda: credential):
            with self.assertRaisesRegex(RuntimeError, "managed identity authentication failed"):
                self.original["get_adme_access_token"]()
            self.assertEqual({}, self.namespace["_ADME_AUTH_STATE"]["tokens"])
            credential.get_token.side_effect = None
            credential.get_token.return_value = self.token("retry-token")
            self.assertEqual("retry-token", self.original["get_adme_access_token"]())

    def test_cli_auth_mode_is_rejected_before_token_acquisition(self):
        with self.patched(ADME_AUTH_METHOD="CLI"):
            with self.assertRaisesRegex(ValueError, "ADME_AUTH_METHOD"):
                self.namespace["_acquire_adme_access_token"]()

    def test_managed_identity_uses_the_optional_user_assigned_client_id(self):
        created = []
        def factory(**kwargs):
            created.append(kwargs)
            return SimpleNamespace(get_token=lambda scope: self.token("mi-token"))
        with self.patched(ADME_AUTH_METHOD="MI", ADME_MANAGED_IDENTITY_CLIENT_ID="client-1", ManagedIdentityCredential=factory):
            self.assertEqual("mi-token", self.namespace["_acquire_adme_access_token"]()[0])
            self.assertEqual("client-1", self.namespace["_adme_token_cache_key"]()[2])
        with self.patched(ADME_AUTH_METHOD="MI", ADME_MANAGED_IDENTITY_CLIENT_ID="", ManagedIdentityCredential=factory):
            self.namespace["_acquire_adme_access_token"]()
        self.assertEqual([{"client_id": "client-1"}, {}], created)

    def test_service_principal_reads_its_secret_from_key_vault(self):
        secrets = []
        applications = []
        notebookutils = SimpleNamespace(credentials=SimpleNamespace(
            getSecret=lambda url, name: secrets.append((url, name)) or "sp-secret"))

        def application(**kwargs):
            applications.append(kwargs)
            return SimpleNamespace(acquire_token_for_client=lambda scopes: {"access_token": "sp-token", "expires_in": 3600})
        with self.patched(ADME_AUTH_METHOD="SP", ADME_TENANT_ID="tenant", ADME_SP_CLIENT_ID="client",
                          ADME_SP_SECRET_KV_NAME="vault", ADME_SP_SECRET_NAME="secret-name",
                          notebookutils=notebookutils, ConfidentialClientApplication=application):
            token, expires_on = self.namespace["_acquire_adme_access_token"]()
        self.assertEqual("sp-token", token)
        self.assertGreater(expires_on, 0)
        self.assertEqual([("https://vault.vault.azure.net/", "secret-name")], secrets)
        self.assertEqual("https://login.microsoftonline.com/tenant", applications[0]["authority"])
        self.assertEqual("sp-secret", applications[0]["client_credential"])

    def test_service_principal_rejects_an_error_result(self):
        application = lambda **kwargs: SimpleNamespace(
            acquire_token_for_client=lambda scopes: {"error": "invalid_client", "error_description": "bad"})
        notebookutils = SimpleNamespace(credentials=SimpleNamespace(getSecret=lambda url, name: "s"))
        with self.patched(ADME_AUTH_METHOD="SP", ADME_TENANT_ID="tenant", ADME_SP_CLIENT_ID="client",
                          ADME_SP_SECRET_KV_NAME="https://vault.example/", ADME_SP_SECRET_NAME="n",
                          notebookutils=notebookutils, ConfidentialClientApplication=application):
            with self.assertRaisesRegex(RuntimeError, "invalid_client"):
                self.namespace["_acquire_adme_access_token"]()

    def test_device_code_flow_prints_the_prompt_and_selects_the_signed_in_account(self):
        account = {"local_account_id": "oid-1", "realm": "tenant-1"}
        state = {"accounts": []}

        class Application:
            def __init__(self, **kwargs):
                pass

            def get_accounts(self):
                return state["accounts"]

            def initiate_device_flow(self, scopes):
                return {"message": "Open the device login page and enter the code."}

            def acquire_token_by_device_flow(self, flow):
                state["accounts"] = [account]
                return {"access_token": "dc-token", "expires_in": 3600,
                        "id_token_claims": {"oid": "oid-1", "tid": "tenant-1"}}

            def acquire_token_silent_with_error(self, scopes, account):
                return {"access_token": "dc-silent", "expires_in": 3600}

        self.namespace["_ADME_AUTH_STATE"]["device_clients"].clear()
        with self.patched(ADME_AUTH_METHOD="DC", ADME_TENANT_ID="tenant-1", ADME_DEVICE_CODE_CLIENT_ID="client",
                          PublicClientApplication=Application):
            self.assertEqual("dc-token", self.namespace["_acquire_adme_access_token"]()[0])
            self.assertEqual("dc-silent", self.namespace["_acquire_adme_access_token"]()[0])

    def test_http_error_detail_is_truncated(self):
        response = SimpleNamespace(text="x" * 1500)
        self.assertEqual(1003, len(self.namespace["_adme_response_detail"](response)))

    def test_workspace_and_lakehouse_ids_resolve_from_explicit_value_then_spark_conf(self):
        namespace = self.namespace
        self.assertEqual("explicit", namespace["_resolve_workspace_id"]("explicit"))
        self.assertEqual("explicit", namespace["_resolve_lakehouse_id"]("explicit"))
        self.spark.conf.set("trident.workspace.id", "workspace-from-conf")
        self.spark.conf.set("trident.lakehouse.id", "lakehouse-from-conf")
        try:
            self.assertEqual("workspace-from-conf", namespace["_resolve_workspace_id"]())
            self.assertEqual("lakehouse-from-conf", namespace["_resolve_lakehouse_id"]())
        finally:
            self.spark.conf.unset("trident.workspace.id")
            self.spark.conf.unset("trident.lakehouse.id")
        with self.assertRaises(ValueError):
            namespace["_resolve_workspace_id"]()
        with self.assertRaises(ValueError):
            namespace["_resolve_lakehouse_id"]()


class OutputShapeHelperTests(NotebookIntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        write_bronze(cls.spark, [row("test:well:1", "1", {"FacilityName": "Alpha"})])
        cls.namespace = run(cls.spark, {
            "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": "docs_", "OUTPUT_DOCS_MODE": "full",
            "WRITE_MODE": "full_refresh", "VERSION_STRATEGY": "merge",
        })

    def test_full_documentation_lists_every_column_of_the_output_table(self):
        documented = {r["column_name"] for r in self.read("silver_output_documentation")
                      .filter("table_name = 'docs_osdu_wks_well'").collect()}
        self.assertIn("data__FacilityName", documented)

    def test_tag_children_are_pivoted_into_prefixed_parent_columns(self):
        parent = self.spark.createDataFrame(
            [("a", "1", 'a:1'), ("b", "1", 'b:1')], "id string, version string, record_key string",
        )
        tags = self.spark.createDataFrame(
            [("a", "1", 'a:1', "env", "dev"), ("a", "1", 'a:1', "team", "x")],
            "id string, version string, record_key string, tag_key string, tag_value string",
        )
        self.assertEqual(self.namespace["_child_type"](tags), "tags")
        row_a = self.namespace["_reassemble_tags"](parent, tags).filter("id = 'a'").collect()[0]
        self.assertEqual(("dev", "x"), (row_a["tag_env"], row_a["tag_team"]))
        self.assertEqual(row_a.record_key, 'a:1')

    def test_primitive_children_are_joined_into_one_delimited_column(self):
        parent = self.spark.createDataFrame([("a", "1", 'a:1')], "id string, version string, record_key string")
        values = self.spark.createDataFrame(
            [("a", "1", 'a:1', 0, "x"), ("a", "1", 'a:1', 1, "y")],
            "id string, version string, record_key string, ordinal int, value string",
        )
        self.assertEqual(self.namespace["_child_type"](values), "primitive")
        result = self.namespace["_reassemble_primitive"](parent, values, "codes").collect()[0]
        self.assertEqual({"x", "y"}, set(result["codes"].split(";")))
        self.assertEqual(result.record_key, 'a:1')

    def test_merge_key_fields_keep_the_source_type_and_default_to_string(self):
        frame = self.spark.createDataFrame([("a", 1)], "id string, version int")
        fields = {f.name: f.dataType.simpleString() for f in self.namespace["_merge_key_fields"](frame)}
        self.assertEqual({"id": "string", "version": "int"}, fields)

    def test_quality_issues_are_written_and_appended(self):
        namespace = self.namespace
        frame = namespace["_single_quality_issue_df"](
            self.spark, "run-1", WELL_KIND, "missing_merge_key_column", "error", "id", "detail")
        self.assertEqual(1, namespace["write_data_quality_issues"](self.spark, frame, "ws", "lh"))
        self.assertEqual(1, namespace["write_data_quality_issues"](self.spark, frame, "ws", "lh"))
        self.assertEqual(0, namespace["write_data_quality_issues"](self.spark, None, "ws", "lh"))
        issues = self.read("silver_data_quality_issues").filter("check_name = 'missing_merge_key_column'")
        self.assertEqual(2, issues.count())

    def test_zero_examples_detect_record_issues_without_emitting_samples(self):
        cases = [
            ("clean", [("a", "1", "{}")], "passed"),
            ("empty", [], "passed"),
            ("null", [(None, "1", "{}")], "issues_found"),
            ("duplicate", [("a", "1", "{}"), ("a", "1", "{}")], "issues_found"),
            ("malformed", [("a", "1", "{broken")], "issues_found"),
        ]
        namespace = self.namespace
        table = "zero_sample_quality"
        with mock.patch.dict(namespace, {
            "data_quality_max_examples": 0, "data_quality_checks": True, "data_quality_issues_table": table,
        }):
            for (name, records, expected), buffered in product(cases, (False, True)):
                with self.subTest(case=name, buffered=buffered):
                    frame = self.spark.createDataFrame(records, "id string, version string, payload string")
                    buffer = [] if buffered else None
                    result = namespace["evaluate_and_write_data_quality_issues"](
                        self.spark, name, WELL_KIND, frame, ["id", "version"], "ws", "lh",
                        quality_issue_rows=buffer,
                    )
                    self.assertEqual((expected, 0), result)
                    if buffered:
                        self.assertEqual([], buffer)
                        namespace["flush_data_quality_issue_rows"](self.spark, buffer, "ws", "lh")
                    self.assertFalse(self.spark.catalog.tableExists(table))

    def test_structural_quality_diagnostics_are_not_sample_capped(self):
        namespace = self.namespace
        frame = self.spark.createDataFrame([], "data__UnlistedA string, data__UnlistedB string")
        registry = SimpleNamespace(has_kind=lambda kind: True, has_field=lambda kind, field: False)
        expected = {
            ("missing_merge_key_column", "id"),
            ("unresolved_schema_field", "data__UnlistedA"),
            ("unresolved_schema_field", "data__UnlistedB"),
        }
        for cap, buffered in product((0, 1), (False, True)):
            with self.subTest(cap=cap, buffered=buffered):
                table = f"structural_quality_{cap}_{int(buffered)}"
                with mock.patch.dict(namespace, {
                    "data_quality_max_examples": cap, "data_quality_checks": True, "data_quality_issues_table": table,
                }):
                    buffer = [] if buffered else None
                    result = namespace["evaluate_and_write_data_quality_issues"](
                        self.spark, "structural", WELL_KIND, frame, ["id"], "ws", "lh",
                        registry=registry, quality_issue_rows=buffer,
                    )
                    self.assertEqual(("issues_found", 3), result)
                    if buffered:
                        self.assertEqual(3, len(buffer))
                        namespace["flush_data_quality_issue_rows"](self.spark, buffer, "ws", "lh")
                        self.assertEqual([], buffer)
                    issues = self.read(table).collect()
                    self.assertEqual(expected, {(issue.check_name, issue.column_name) for issue in issues})
                    self.assertTrue(all(issue.source_id is None and issue.source_version is None for issue in issues))

    def test_positive_example_caps_apply_per_check_and_column(self):
        namespace = self.namespace
        records = [(f"record-{i}", "1", "{broken", "[broken") for i in range(4) for _ in range(2)]
        records.extend((None, str(i), "{broken", "[broken") for i in range(4))
        frame = self.spark.createDataFrame(records, "id string, version string, payload string, extra string")
        expected = {
            ("null_merge_key", "id"): 2,
            ("duplicate_merge_key", "id,version"): 2,
            ("malformed_json", "payload"): 2,
            ("malformed_json", "extra"): 2,
        }
        for buffered in (False, True):
            with self.subTest(buffered=buffered):
                table = f"positive_sample_quality_{int(buffered)}"
                with mock.patch.dict(namespace, {
                    "data_quality_max_examples": 2, "data_quality_checks": True, "data_quality_issues_table": table,
                }):
                    buffer = [] if buffered else None
                    result = namespace["evaluate_and_write_data_quality_issues"](
                        self.spark, "positive", WELL_KIND, frame, ["id", "version"], "ws", "lh",
                        quality_issue_rows=buffer,
                    )
                    self.assertEqual(("issues_found", 8), result)
                    if buffered:
                        namespace["flush_data_quality_issue_rows"](self.spark, buffer, "ws", "lh")
                    counts = self.read(table).groupBy("check_name", "column_name").count().collect()
                    self.assertEqual(expected, {(r.check_name, r.column_name): r["count"] for r in counts})
                    duplicate = self.read(table).where("check_name = 'duplicate_merge_key'").first()
                    self.assertEqual(WELL_KIND, duplicate.source_kind)
                    self.assertEqual("1", duplicate.source_version)
                    self.assertIn(duplicate.source_id, duplicate.issue_detail)

    def test_disabled_quality_checks_do_not_detect_or_emit(self):
        namespace = self.namespace
        frame = self.spark.createDataFrame([(None, "{broken")], "id string, payload string")
        for cap, buffered in product((0, 1), (False, True)):
            with self.subTest(cap=cap, buffered=buffered), mock.patch.dict(namespace, {
                "data_quality_max_examples": cap, "data_quality_checks": False,
                "collect_data_quality_issues": mock.Mock(side_effect=AssertionError("disabled checks evaluated")),
            }):
                buffer = [] if buffered else None
                self.assertEqual(
                    ("disabled", 0),
                    namespace["evaluate_and_write_data_quality_issues"](
                        self.spark, "disabled", WELL_KIND, frame, ["id", "version"], "ws", "lh",
                        quality_issue_rows=buffer,
                    ),
                )
                if buffered:
                    self.assertEqual([], buffer)

    def test_zero_example_status_reaches_run_metadata(self):
        from pyspark.sql import functions as F

        frame = self.spark.createDataFrame([row("test:well:quality", "1", {"FacilityName": "Alpha"})], BRONZE_DDL)
        frame.withColumn("createUser", F.lit("{broken")).write.format("delta").mode("overwrite").saveAsTable("osducatalog")
        namespace = run(self.spark, {
            "RUN_PROFILE": "execute", "KINDS": [WELL_KIND], "TABLE_PREFIX": "zero_status_",
            "WRITE_MODE": "full_refresh", "VERSION_STRATEGY": "merge",
            "DATA_QUALITY_MAX_EXAMPLES": 0, "DATA_QUALITY_ISSUES_TABLE": "zero_status_quality",
        })
        result = namespace["results"][0]
        self.assertEqual(("issues_found", 0), (result.quality_status, result.quality_issue_count))
        run_id = self.read("silver_run_manifest").where(F.col("parent_table") == result.parent_table).first().run_id
        for table in ("silver_run_info", "silver_run_manifest"):
            metadata = self.read(table).where(F.col("run_id") == run_id).collect()
            self.assertTrue(metadata)
            self.assertTrue(all((r.quality_status, r.quality_issue_count) == ("issues_found", 0) for r in metadata))
        self.assertFalse(self.spark.catalog.tableExists("zero_status_quality"))


if __name__ == "__main__":
    unittest.main()
