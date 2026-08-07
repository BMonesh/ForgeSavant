import csv
import json
import sys
from pathlib import Path
import tempfile
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from build_coverage_queue import build_coverage_queue  # noqa: E402


class CoverageQueueTests(unittest.TestCase):
    def test_prioritizes_actionable_uncovered_products(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identities = root / "identities"
            cleaned = root / "cleaned"
            lake = root / "lake"
            identities.mkdir()
            cleaned.mkdir()
            for category, filename in {
                "processors": "processors.json", "gpus": "gpus.json",
                "motherboards": "motherboards.json", "ram": "ram.json",
                "storage": "storage.json", "power_supplies": "power-supplies.json",
                "cabinets": "cabinets.json",
            }.items():
                rows = [{
                    "name": "Example CPU",
                    "manufacturerPartNumber": "CPU-1",
                    "sourceUrl": "https://manufacturer.example/cpu-1",
                }] if category == "processors" else []
                (identities / filename).write_text(json.dumps(rows), encoding="utf-8")
                csv_name = f"{category}_cleaned.csv"
                with (cleaned / csv_name).open("w", encoding="utf-8", newline="") as target:
                    writer = csv.DictWriter(target, fieldnames=["name", "manufacturer"])
                    writer.writeheader()
                    if rows:
                        writer.writerow({"name": "Example CPU", "manufacturer": "Example"})

            coverage = root / "coverage.json"
            coverage.write_text(json.dumps({"records": [{
                "component": "processors", "manufacturer_part_number": "CPU-1", "status": "restricted",
            }]}), encoding="utf-8")
            output_json = root / "queue.json"
            output_csv = root / "queue.csv"
            result = build_coverage_queue(identities, cleaned, lake, coverage, output_json, output_csv)

            self.assertEqual(result["counts"]["verified"], 1)
            self.assertEqual(result["counts"]["manufacturerReady"], 1)
            self.assertEqual(result["records"][0]["manufacturer"], "Example")
            self.assertEqual(result["records"][0]["latestIcecatStatus"], "restricted")
            self.assertGreater(result["records"][0]["priority"], 0)
            self.assertTrue(output_csv.exists())


if __name__ == "__main__":
    unittest.main()
