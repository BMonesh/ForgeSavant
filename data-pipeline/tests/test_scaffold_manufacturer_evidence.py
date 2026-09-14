import json
import sys
from pathlib import Path
import tempfile
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from scaffold_manufacturer_evidence import (  # noqa: E402
    build_scaffold,
    specification_fields,
)
from ingest_manufacturer_evidence import build_manufacturer_observation  # noqa: E402


QUEUE = {
    "schemaVersion": "1.0",
    "generatedAt": "2026-08-07T14:34:26.522401+00:00",
    "records": [
        {
            "category": "processors",
            "catalogName": "Example CPU",
            "manufacturer": "Example",
            "manufacturerPartNumber": "CPU-1",
            "status": "manufacturer_ready",
            "latestIcecatStatus": "restricted",
            "gapReason": "Open Icecat access is restricted.",
            "priority": 100.0,
        },
        {
            "category": "processors",
            "catalogName": "Covered CPU",
            "manufacturer": "Example",
            "manufacturerPartNumber": "CPU-2",
            "status": "covered",
            "priority": 10.0,
        },
        {
            "category": "gpus",
            "catalogName": "Example GPU",
            "manufacturer": "Example",
            "manufacturerPartNumber": "GPU-1",
            "status": "manufacturer_ready",
            "priority": 50.0,
        },
        {
            "category": "gpus",
            "catalogName": "Ghost GPU",
            "manufacturer": "Example",
            "manufacturerPartNumber": "GPU-MISSING",
            "status": "manufacturer_ready",
            "priority": 40.0,
        },
    ],
}

IDENTITIES = {
    "processors.json": [
        {"name": "Example CPU", "manufacturerPartNumber": "CPU-1", "sourceUrl": "https://manufacturer.example/cpu-1"},
        {"name": "Covered CPU", "manufacturerPartNumber": "CPU-2", "sourceUrl": "https://manufacturer.example/cpu-2"},
    ],
    "gpus.json": [
        {"name": "Example GPU", "manufacturerPartNumber": "GPU-1", "sourceUrl": "https://manufacturer.example/gpu-1"},
    ],
    "motherboards.json": [],
    "ram.json": [],
    "storage.json": [],
    "power-supplies.json": [],
    "cabinets.json": [],
}


class ScaffoldFixture:
    def __init__(self, directory: Path):
        self.queue_path = directory / "catalog_coverage_queue.json"
        self.queue_path.write_text(json.dumps(QUEUE), encoding="utf-8")

        self.identity_dir = directory / "verified_identity"
        self.identity_dir.mkdir()
        for filename, rows in IDENTITIES.items():
            (self.identity_dir / filename).write_text(json.dumps(rows), encoding="utf-8")

        self.cleaned_dir = directory / "cleaned_data"
        self.cleaned_dir.mkdir()
        (self.cleaned_dir / "processors_cleaned.csv").write_text(
            "name,type,manufacturer,cores,socket,tdp,price,source\n", encoding="utf-8"
        )

    def build(self, **kwargs):
        return build_scaffold(self.queue_path, self.identity_dir, self.cleaned_dir, **kwargs)


class SpecificationFieldTests(unittest.TestCase):
    def test_drops_offer_and_provenance_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            cleaned = Path(directory)
            (cleaned / "storage_cleaned.csv").write_text(
                "name,type,manufacturer,capacity,interface,price,source_url,currency,collected_at,data_status\n",
                encoding="utf-8",
            )
            self.assertEqual(specification_fields(cleaned, "storage"), ["capacity", "interface"])

    def test_falls_back_to_compatibility_fields_without_a_cleaned_file(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(specification_fields(Path(directory), "cabinets"), ["motherboard_support"])

    def test_appends_missing_compatibility_critical_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            cleaned = Path(directory)
            (cleaned / "ram_cleaned.csv").write_text("name,manufacturer,capacity,price\n", encoding="utf-8")
            self.assertEqual(specification_fields(cleaned, "ram"), ["capacity", "ram_type"])


class ScaffoldTests(unittest.TestCase):
    def test_scaffolds_only_manufacturer_ready_products(self):
        with tempfile.TemporaryDirectory() as directory:
            scaffold = ScaffoldFixture(Path(directory)).build()
            names = [record["catalogName"] for record in scaffold["records"]]
            self.assertEqual(names, ["Example CPU", "Example GPU"])

    def test_reports_products_absent_from_the_identity_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            scaffold = ScaffoldFixture(Path(directory)).build()
            self.assertEqual(
                scaffold["skipped"],
                [{
                    "category": "gpus",
                    "manufacturerPartNumber": "GPU-MISSING",
                    "reason": "not in the verified identity manifest",
                }],
            )

    def test_takes_the_source_url_from_the_identity_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            scaffold = ScaffoldFixture(Path(directory)).build()
            record = scaffold["records"][0]
            self.assertEqual(record["officialSourceUrl"], "https://manufacturer.example/cpu-1")
            # The ingester demands an exact match, so a scaffolded URL must satisfy it.
            observation = build_manufacturer_observation(
                {**record, "observedAt": "2026-08-07T00:00:00+00:00", "specifications": {"socket": "AM4"}},
                IDENTITIES["processors.json"][0],
            )
            self.assertEqual(observation["source"], "manufacturer_manufacturer_example")

    def test_leaves_every_specification_untranscribed(self):
        with tempfile.TemporaryDirectory() as directory:
            scaffold = ScaffoldFixture(Path(directory)).build()
            specifications = scaffold["records"][0]["specifications"]
            self.assertEqual(sorted(specifications), ["cores", "socket", "tdp"])
            self.assertTrue(all(value is None for value in specifications.values()))
            self.assertEqual(scaffold["records"][0]["observedAt"], "")

    def test_a_scaffold_cannot_be_ingested_before_it_is_filled_in(self):
        with tempfile.TemporaryDirectory() as directory:
            scaffold = ScaffoldFixture(Path(directory)).build()
            record = {**scaffold["records"][0], "observedAt": "2026-08-07T00:00:00+00:00"}
            with self.assertRaisesRegex(ValueError, "untranscribed fields"):
                build_manufacturer_observation(record, IDENTITIES["processors.json"][0])

    def test_partially_filled_specifications_still_fail_on_the_remaining_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            scaffold = ScaffoldFixture(Path(directory)).build()
            record = {
                **scaffold["records"][0],
                "observedAt": "2026-08-07T00:00:00+00:00",
                "specifications": {"cores": 6, "socket": None, "tdp": ""},
            }
            with self.assertRaisesRegex(ValueError, "socket, tdp"):
                build_manufacturer_observation(record, IDENTITIES["processors.json"][0])

    def test_filters_by_category_and_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = ScaffoldFixture(Path(directory))
            self.assertEqual(
                [record["category"] for record in fixture.build(categories={"gpus"})["records"]],
                ["gpus"],
            )
            limited = fixture.build(limit=1)["records"]
            self.assertEqual([record["catalogName"] for record in limited], ["Example CPU"])

    def test_carries_capture_guidance_for_the_reviewer(self):
        with tempfile.TemporaryDirectory() as directory:
            scaffold = ScaffoldFixture(Path(directory)).build()
            capture = scaffold["records"][0]["_capture"]
            self.assertEqual(capture["latestIcecatStatus"], "restricted")
            self.assertEqual(capture["compatibilityCritical"], ["socket", "tdp"])


if __name__ == "__main__":
    unittest.main()
