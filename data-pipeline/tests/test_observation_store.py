import json
import sys
from pathlib import Path
import tempfile
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from observation_store import ObservationStore, observation_id, redact, validate_observation  # noqa: E402


def observation(**overrides):
    value = {
        "schema_version": "1.0",
        "observation_kind": "product_content",
        "source": "open_icecat",
        "source_tier": "open",
        "source_product_id": "29900045",
        "manufacturer": "iiyama",
        "manufacturer_part_number": "X4071UHSU-B1",
        "name": "Example monitor",
        "category": "Computer Monitors",
        "catalog_category": "gpus",
        "gtins": ["4948570114344"],
        "image_url": "https://images.example/product.jpg",
        "manufacturer_url": "https://manufacturer.example/product",
        "source_record_url": "https://data.example/product",
        "specifications": {"display_resolution": "3840 x 2160"},
        "observed_at": "2026-07-22T00:00:00+00:00",
        "raw_sha256": "a" * 64,
    }
    value.update(overrides)
    return value


class ObservationStoreTests(unittest.TestCase):
    def test_validates_canonical_product_content(self):
        self.assertEqual(validate_observation(observation()), [])
        errors = validate_observation(observation(manufacturer_part_number="", observed_at="yesterday"))
        self.assertIn("manufacturer_part_number is required for product content", errors)
        self.assertIn("observed_at must be an ISO-8601 timestamp with timezone", errors)

    def test_product_content_identity_is_stable_across_collection_times(self):
        first = observation_id(observation())
        later = observation_id(observation(observed_at="2026-07-23T00:00:00+00:00"))
        self.assertEqual(first, later)

    def test_product_content_identity_changes_when_catalog_identity_is_corrected(self):
        first = observation_id(observation())
        corrected = observation_id(observation(manufacturer_part_number="CORRECTED-MPN"))
        self.assertNotEqual(first, corrected)

    def test_writes_immutable_manifest_and_deduplicates_across_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ObservationStore(Path(directory))
            first = store.ingest("open_icecat", [observation()], run_id="run-00000001")
            second = store.ingest("open_icecat", [observation()], run_id="run-00000002")

            self.assertEqual(first.accepted, 1)
            self.assertEqual(second.accepted, 0)
            self.assertEqual(second.duplicates, 1)
            manifest = json.loads(Path(first.manifest_path).read_text(encoding="utf-8"))
            self.assertEqual(manifest["counts"]["accepted"], 1)
            self.assertEqual(len(manifest["checksums"]["normalized"]), 64)

    def test_quarantines_invalid_records_and_redacts_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ObservationStore(Path(directory))
            result = store.ingest(
                "open_icecat",
                [observation(source="other_source", password="never-store-this")],
                run_id="run-00000003",
            )
            self.assertEqual(result.quarantined, 1)
            manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
            quarantine = Path(manifest["paths"]["quarantine"]).read_text(encoding="utf-8")
            self.assertIn("[redacted]", quarantine)
            self.assertNotIn("never-store-this", quarantine)

    def test_redacts_nested_sensitive_values(self):
        self.assertEqual(redact({"nested": {"api_key": "secret"}})["nested"]["api_key"], "[redacted]")


if __name__ == "__main__":
    unittest.main()
