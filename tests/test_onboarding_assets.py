import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
GUID_PATTERN = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


class OnboardingAssetTests(unittest.TestCase):
    def test_sample_json_files_are_valid_and_placeholder_based(self) -> None:
        sample_files = sorted(SAMPLES.glob("**/*.json"))
        self.assertGreaterEqual(len(sample_files), 5)
        for path in sample_files:
            raw = path.read_text(encoding="utf-8")
            json.loads(raw)
            self.assertIsNone(GUID_PATTERN.search(raw), f"{path} should not contain concrete GUIDs")
            self.assertNotIn("client_secret", raw.lower())
            self.assertNotIn("password", raw.lower())

    def test_config_samples_have_settings_objects(self) -> None:
        for path in (SAMPLES / "config").glob("*.json"):
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertIn("description", payload)
            self.assertIn("settings", payload)
            self.assertIn("RUN_PROFILE", payload["settings"])

    def test_synthetic_bronze_records_have_required_columns(self) -> None:
        records = json.loads((SAMPLES / "synthetic_bronze_records.json").read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(records), 2)
        for record in records:
            for column in ["id", "version", "kind", "isActive", "data"]:
                self.assertIn(column, record)
            self.assertTrue(record["isActive"])
            json.loads(record["data"])


if __name__ == "__main__":
    unittest.main()
