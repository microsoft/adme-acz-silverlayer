"""Notebook run on real Spark/Delta with public OSDU TNO records against a local fake ADME schema service.

Bronze rows are regenerated from the OSDU TNO open test data and schemas come from the public OSDU
data-definitions project; only the ADME endpoint and its token are fake. Needs network access to
community.opengroup.org on the first run (downloads are cached in the system temporary directory).
"""

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fake_adme import FakeAdme
from notebook_runner import run_notebook
from test_notebook_integration import NotebookIntegrationBase
from tno_bronze import write_bronze

PARTITION = "test-partition"
TOKEN = "fake-adme-token"
RECORDS = int(os.environ.get("ADME_ACZ_TNO_RECORDS", "10"))
CACHE = Path(tempfile.gettempdir()) / "adme-acz-tno-cache"


class TnoFakeAdmeRunTests(NotebookIntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        bronze_path = Path(cls.temporary.name) / "tno-osducatalog"
        write_bronze(cls.spark, bronze_path.as_posix(), RECORDS, RECORDS, PARTITION, CACHE)
        cls.spark.sql(f"CREATE TABLE osducatalog USING delta LOCATION '{bronze_path.as_posix()}'")

        cls.fake = FakeAdme(PARTITION, TOKEN, CACHE)
        cls.fake.__enter__()
        cls.addClassCleanup(cls.fake.__exit__, None, None, None)
        ca_bundle = mock.patch.dict(os.environ, {"REQUESTS_CA_BUNDLE": str(cls.fake.certificate_path)})
        ca_bundle.start()
        cls.addClassCleanup(ca_bundle.stop)

        def stub_token(namespace):
            namespace["get_adme_access_token"] = lambda: TOKEN

        cls.namespace = run_notebook(cls.spark, {
            "ADME_ENDPOINT": cls.fake.endpoint,
            "ADME_DATA_PARTITION_ID": PARTITION,
            "ADME_AUTH_METHOD": "MI",
            "RUN_PROFILE": "execute",
            "KINDS": ["osdu:wks:master-data--Well:*", "osdu:wks:master-data--Wellbore:*"],
            "TABLE_PREFIX": "tno_",
            "ALLOW_OVERWRITE": True,
            "WRITE_MODE": "full_refresh",
            "VERSION_STRATEGY": "merge",
        }, before_pipeline=stub_token)

    def test_schemas_were_fetched_over_https_with_token_and_partition(self):
        paths = {request["path"] for request in self.fake.requests}
        self.assertIn("/api/schema-service/v1/schema/osdu:wks:master-data--Well:1.0.0", paths)
        self.assertIn("/api/schema-service/v1/schema/osdu:wks:master-data--Wellbore:1.0.0", paths)

    def test_every_kind_was_published(self):
        self.assertEqual(2, len(self.namespace["results"]))

    def test_parent_tables_hold_every_active_tno_record(self):
        active = self.read("osducatalog").filter("isActive = true")
        for kind_fragment, table in (("master-data--Well:", "tno_osdu_wks_well"),
                                     ("master-data--Wellbore:", "tno_osdu_wks_wellbore")):
            expected = active.filter(f"kind like '%{kind_fragment}%'").select("id", "version").distinct().count()
            self.assertEqual(RECORDS, expected)
            self.assertEqual(expected, self.read(table).select("id", "version").distinct().count())

    def test_schema_typed_columns_are_populated(self):
        wells = self.read("tno_osdu_wks_well")
        self.assertIn("data__FacilityName", wells.columns)
        self.assertEqual(0, wells.filter("data__FacilityName is null").count())

    def test_array_properties_become_child_tables(self):
        aliases = self.read("tno_osdu_wks_well___namealiases")
        self.assertGreater(aliases.count(), 0)
        self.assertIn("id", aliases.columns)


if __name__ == "__main__":
    unittest.main()
