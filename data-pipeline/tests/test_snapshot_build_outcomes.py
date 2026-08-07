import sys
from datetime import datetime, timezone
from pathlib import Path
import unittest

from bson import ObjectId


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from observation_store import validate_observation  # noqa: E402
from snapshot_build_outcomes import build_outcome_observation  # noqa: E402


class BuildOutcomeSnapshotTests(unittest.TestCase):
    def test_builds_valid_privacy_safe_outcome(self):
        row = build_outcome_observation({
            "_id": ObjectId(),
            "eventType": "build_saved",
            "subjectHash": "a" * 64,
            "buildHash": "b" * 64,
            "componentIds": {"processor": ObjectId(), "gpu": ObjectId()},
            "buildTotal": 89999,
            "currency": "INR",
            "compatibilityStatus": "compatible",
            "compatibilityEngineVersion": "compat-1",
            "analyticsModelVersion": "planning-1",
            "occurredAt": datetime(2026, 7, 23, tzinfo=timezone.utc),
        })
        self.assertEqual(validate_observation(row), [])
        self.assertEqual(row["source"], "forgesavant_app")
        self.assertNotIn("email", row)
        self.assertNotIn("name", row)


if __name__ == "__main__":
    unittest.main()
