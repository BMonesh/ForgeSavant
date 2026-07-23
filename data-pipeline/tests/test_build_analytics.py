import json
import sys
from pathlib import Path
import tempfile
import unittest

import duckdb


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from build_analytics import build_analytics  # noqa: E402


class BuildAnalyticsTests(unittest.TestCase):
    def test_builds_reconciled_duckdb_parquet_and_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake = root / "lake"
            run_id = "run-analytics-001"
            normalized = lake / "normalized" / "2026-07-22" / run_id
            manifests = lake / "manifests"
            identities = root / "identities"
            normalized.mkdir(parents=True)
            manifests.mkdir(parents=True)
            identities.mkdir(parents=True)

            record = {
                "observation_id": "b" * 64, "schema_version": "1.0",
                "observation_kind": "product_content", "source": "open_icecat", "source_tier": "open",
                "source_product_id": "1", "catalog_category": "processors", "catalog_name": "Example CPU",
                "manufacturer": "Example", "manufacturer_part_number": "CPU-1", "name": "Example CPU",
                "category": "Processors", "gtins": ["123"], "specifications": {"cores": "8"},
                "image_url": "https://example.test/image.jpg", "manufacturer_url": "", "source_record_url": "",
                "observed_at": "2026-07-22T00:00:00+00:00", "ingested_at": "2026-07-22T00:01:00+00:00",
                "raw_sha256": "a" * 64,
            }
            corrected = {
                **record,
                "observation_id": "c" * 64,
                "manufacturer_part_number": "CPU-1-CORRECTED",
                "ingested_at": "2026-07-22T00:02:00+00:00",
            }
            (normalized / "observations.jsonl").write_text(
                json.dumps(record) + "\n" + json.dumps(corrected) + "\n", encoding="utf-8"
            )
            manifest = {
                "run_id": run_id, "source": "open_icecat", "received_at": "2026-07-22T00:01:00+00:00",
                "counts": {"received": 2, "accepted": 2, "duplicates": 0, "quarantined": 0},
                "checksums": {"raw": "1", "normalized": "2", "quarantine": "3"},
            }
            (manifests / f"{run_id}.json").write_text(json.dumps(manifest), encoding="utf-8")

            filenames = ["processors.json", "gpus.json", "motherboards.json", "ram.json", "storage.json", "power-supplies.json", "cabinets.json"]
            for filename in filenames:
                rows = [{"name": "Example CPU"}] if filename == "processors.json" else []
                (identities / filename).write_text(json.dumps(rows), encoding="utf-8")

            coverage = root / "coverage.json"
            coverage.write_text(json.dumps({"source": "open_icecat", "records": [{
                "component": "processors", "catalog_name": "Example CPU", "manufacturer": "Example",
                "manufacturer_part_number": "CPU-1", "status": "available", "error": "",
            }]}), encoding="utf-8")

            analytics = root / "analytics"
            summary = build_analytics(lake, analytics, coverage, identities)
            self.assertEqual(summary["catalog"]["verifiedProducts"], 1)
            self.assertEqual(summary["pipeline"]["accepted"], 2)
            self.assertEqual(summary["catalog"]["observedProducts"], 1)
            self.assertEqual(summary["quality"]["identityCompletenessRate"], 1.0)
            self.assertTrue((analytics / "parquet" / "catalog_observations.parquet").exists())

            connection = duckdb.connect(str(analytics / "forgesavant.duckdb"), read_only=True)
            self.assertEqual(connection.execute("SELECT count(*) FROM catalog_observations").fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT count(*) FROM current_catalog_observations").fetchone()[0], 1)
            self.assertEqual(
                connection.execute("SELECT manufacturer_part_number FROM current_catalog_observations").fetchone()[0],
                "CPU-1-CORRECTED",
            )
            connection.close()


if __name__ == "__main__":
    unittest.main()
