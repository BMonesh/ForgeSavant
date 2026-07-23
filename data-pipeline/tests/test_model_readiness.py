import json
from pathlib import Path
import sys
import tempfile
import unittest

import duckdb


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from analyze_model_readiness import analyze  # noqa: E402


class ModelReadinessTests(unittest.TestCase):
    def test_blocks_predictive_models_without_labels_or_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "analytics.duckdb"
            connection = duckdb.connect(str(database))
            connection.execute("""
                CREATE TABLE catalog_observations (
                  observation_id VARCHAR, manufacturer_part_number VARCHAR,
                  catalog_category VARCHAR, observed_at TIMESTAMPTZ,
                  ingested_at TIMESTAMPTZ, specifications_json JSON
                );
                INSERT INTO catalog_observations VALUES
                  ('obs-1', 'GPU-1', 'gpus', '2026-07-20', '2026-07-21', '{"memory":"8 GB","cores":"100"}');
                CREATE VIEW current_catalog_observations AS SELECT * FROM catalog_observations;
                CREATE TABLE ingestion_runs (received_at TIMESTAMPTZ);
                INSERT INTO ingestion_runs VALUES ('2026-07-21');
            """)
            connection.close()
            summary_path = root / "summary.json"
            summary_path.write_text(json.dumps({
                "grain": "one observation per product content version",
                "catalog": {"verifiedProducts": 2, "observedProducts": 1},
                "pipeline": {"runs": 1},
                "categories": {
                    "gpus": {"verifiedCatalogProducts": 1},
                    "processors": {"verifiedCatalogProducts": 1},
                },
            }), encoding="utf-8")

            result = analyze(database, summary_path)

            self.assertEqual(result["dataset"]["catalogCoverageRate"], 0.5)
            self.assertTrue(result["checks"]["uniqueObservationIds"])
            self.assertFalse(result["checks"]["allCategoriesObserved"])
            self.assertFalse(result["checks"]["supervisedOutcomeLabelsPresent"])
            self.assertFalse(result["checks"]["temporalHistoryAtLeastEightDates"])
            self.assertEqual([item["status"] for item in result["uses"]], ["ready", "limited", "blocked", "blocked"])


if __name__ == "__main__":
    unittest.main()
