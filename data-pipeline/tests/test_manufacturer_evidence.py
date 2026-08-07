import json
import sys
from pathlib import Path
import tempfile
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from ingest_manufacturer_evidence import (  # noqa: E402
    build_manufacturer_observation,
    ingest_evidence,
)


IDENTITY = {
    "name": "Example CPU",
    "manufacturerPartNumber": "CPU-1",
    "sourceUrl": "https://manufacturer.example/products/cpu-1",
}


def evidence(**overrides):
    value = {
        "category": "processors",
        "catalogName": "Example CPU",
        "manufacturer": "Example",
        "manufacturerPartNumber": "CPU-1",
        "officialSourceUrl": IDENTITY["sourceUrl"],
        "observedAt": "2026-07-23T00:00:00+00:00",
        "specifications": {"cores": 8},
    }
    value.update(overrides)
    return value


class ManufacturerEvidenceTests(unittest.TestCase):
    def test_builds_source_neutral_observation_from_verified_url(self):
        row = build_manufacturer_observation(evidence(), IDENTITY)
        self.assertEqual(row["source"], "manufacturer_manufacturer_example")
        self.assertEqual(row["source_tier"], "manufacturer")
        self.assertEqual(len(row["raw_sha256"]), 64)

    def test_rejects_unverified_url(self):
        with self.assertRaisesRegex(ValueError, "exactly match"):
            build_manufacturer_observation(
                evidence(officialSourceUrl="https://unverified.example/cpu-1"),
                IDENTITY,
            )

    def test_ingests_valid_records_and_reports_unknown_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identities = root / "identities"
            identities.mkdir()
            filenames = {
                "processors.json", "gpus.json", "motherboards.json", "ram.json",
                "storage.json", "power-supplies.json", "cabinets.json",
            }
            for filename in filenames:
                rows = [IDENTITY] if filename == "processors.json" else []
                (identities / filename).write_text(json.dumps(rows), encoding="utf-8")
            feed = root / "feed.json"
            feed.write_text(json.dumps({
                "schemaVersion": "1.0",
                "records": [
                    evidence(),
                    evidence(manufacturerPartNumber="NOT-VERIFIED"),
                ],
            }), encoding="utf-8")

            result = ingest_evidence(feed, identities, root / "lake")
            self.assertEqual(result["ingestions"][0]["accepted"], 1)
            self.assertEqual(len(result["rejected"]), 1)


if __name__ == "__main__":
    unittest.main()
