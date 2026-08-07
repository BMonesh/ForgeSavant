import sys
from datetime import datetime, timezone
from pathlib import Path
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from snapshot_retail_offers import build_retail_observation  # noqa: E402


class RetailSnapshotTests(unittest.TestCase):
    def test_builds_valid_immutable_offer(self):
        component = {
            "name": "Example GPU",
            "manufacturer": "Example",
            "identity": {"manufacturerPartNumber": "GPU-1"},
        }
        history = {
            "price": 24999,
            "currency": "INR",
            "availability": "in_stock",
            "source": "authorized-store",
            "sourceUrl": "https://retailer.example/gpu-1",
            "sourceItemId": "sku-1",
            "observedAt": datetime(2026, 7, 23, tzinfo=timezone.utc),
            "importChecksum": "a" * 64,
        }
        row = build_retail_observation(component, history, "gpus")
        self.assertEqual(row["observation_kind"], "retail_offer")
        self.assertEqual(row["price"], 24999)
        self.assertEqual(row["source"], "authorized_store")
        self.assertEqual(len(row["raw_sha256"]), 64)

    def test_excludes_seed_history_without_retailer_item_id(self):
        with self.assertRaisesRegex(ValueError, "seed price history"):
            build_retail_observation(
                {"name": "Example", "manufacturer": "Example", "identity": {"manufacturerPartNumber": "CPU-1"}},
                {"price": 100, "source": "catalog", "recordedAt": "2026-07-23T00:00:00+00:00"},
                "processors",
            )


if __name__ == "__main__":
    unittest.main()
