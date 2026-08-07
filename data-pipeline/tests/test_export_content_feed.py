import json
import sys
from pathlib import Path
import tempfile
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from export_content_feed import export_feed  # noqa: E402


class ExportContentFeedTests(unittest.TestCase):
    def test_latest_logical_observation_supersedes_an_identity_correction(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            older = root / "2026-07-21" / "run-old" / "observations.jsonl"
            newer = root / "2026-07-22" / "run-new" / "observations.jsonl"
            older.parent.mkdir(parents=True)
            newer.parent.mkdir(parents=True)
            base = {
                "observation_kind": "product_content",
                "source": "open_icecat",
                "catalog_category": "gpus",
                "catalog_name": "Example GPU",
                "source_product_id": "123",
            }
            older.write_text(json.dumps({
                **base,
                "observation_id": "a" * 64,
                "manufacturer_part_number": "REGIONAL-SKU",
                "ingested_at": "2026-07-21T00:00:00+00:00",
            }) + "\n", encoding="utf-8")
            newer.write_text(json.dumps({
                **base,
                "observation_id": "b" * 64,
                "manufacturer_part_number": "VERIFIED-MPN",
                "ingested_at": "2026-07-22T00:00:00+00:00",
            }) + "\n" + json.dumps({
                **base,
                "observation_kind": "benchmark",
                "source": "blender_open_data",
                "source_product_id": "benchmark-1",
                "manufacturer_part_number": "VERIFIED-MPN",
                "ingested_at": "2026-07-22T00:01:00+00:00",
            }) + "\n", encoding="utf-8")
            output = root / "feed.json"

            payload = export_feed(root, output)

            self.assertEqual(len(payload["observations"]), 1)
            self.assertEqual(payload["source"], "forgesavant_product_content")
            self.assertEqual(payload["source_counts"], {"open_icecat": 1})
            self.assertEqual(payload["observations"][0]["manufacturer_part_number"], "VERIFIED-MPN")


if __name__ == "__main__":
    unittest.main()
