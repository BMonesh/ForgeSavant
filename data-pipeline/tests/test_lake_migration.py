import hashlib
import json
import sys
from pathlib import Path
import tempfile
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from observation_store import MongoObservationStore, ObservationStore  # noqa: E402
from migrate_lake_to_mongo import migrate_lake  # noqa: E402
from publish_reports import collect_reports, publish_reports  # noqa: E402
from test_mongo_observation_store import FakeBulkWriteError, FakeDatabase, observation  # noqa: E402


def seeded_lake(root: Path, count: int = 3) -> ObservationStore:
    store = ObservationStore(root)
    store.ingest("test_source", [
        observation(source_product_id=f"SKU-{index}", raw_sha256=hashlib.sha256(f"p{index}".encode()).hexdigest())
        for index in range(count)
    ])
    return store


def install_fake_pymongo_errors():
    """The store imports BulkWriteError lazily; point it at the fake."""
    module = type(sys)("pymongo.errors")
    module.BulkWriteError = FakeBulkWriteError
    sys.modules["pymongo.errors"] = module


class LakeMigrationTests(unittest.TestCase):
    def setUp(self):
        install_fake_pymongo_errors()

    def tearDown(self):
        sys.modules.pop("pymongo.errors", None)

    def test_dry_run_reports_pending_work_without_writing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seeded_lake(root)
            store = MongoObservationStore(FakeDatabase())
            summary = migrate_lake(root, store, apply=False)

            self.assertEqual(summary["pending"]["observations"], 3)
            self.assertEqual(summary["pending"]["runs"], 1)
            self.assertFalse(summary["applied"])
            self.assertEqual(list(store.read_observations()), [])

    def test_apply_copies_observations_runs_and_side_records(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seeded_lake(root)
            store = MongoObservationStore(FakeDatabase())
            summary = migrate_lake(root, store, apply=True)

            self.assertEqual(summary["copied"]["observations"], 3)
            self.assertEqual(summary["mongoTotals"]["observations"], 3)
            self.assertEqual(summary["mongoTotals"]["runs"], 1)

    def test_rerunning_the_migration_copies_nothing_further(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seeded_lake(root)
            store = MongoObservationStore(FakeDatabase())
            migrate_lake(root, store, apply=True)
            second = migrate_lake(root, store, apply=True)

            self.assertEqual(second["pending"]["observations"], 0)
            self.assertEqual(second["pending"]["runs"], 0)
            self.assertEqual(second["mongoTotals"]["observations"], 3)

    def test_migration_resumes_after_a_partial_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local = seeded_lake(root)
            store = MongoObservationStore(FakeDatabase())
            store.ensure_indexes()
            # One observation already landed; the rest must still migrate.
            first = next(iter(local.read_observations()))
            store._collection("normalized").insert_one(dict(first))

            summary = migrate_lake(root, store, apply=True)
            self.assertEqual(summary["pending"]["observations"], 2)
            self.assertEqual(summary["mongoTotals"]["observations"], 3)

    def test_migrated_observations_deduplicate_a_later_ingestion(self):
        """The whole point: a scheduled run must not re-accept migrated history."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seeded_lake(root)
            store = MongoObservationStore(FakeDatabase())
            migrate_lake(root, store, apply=True)

            repeat = store.ingest("test_source", [
                observation(source_product_id=f"SKU-{index}", raw_sha256=hashlib.sha256(f"p{index}".encode()).hexdigest())
                for index in range(3)
            ])
            self.assertEqual((repeat.accepted, repeat.duplicates), (0, 3))


class PublishReportsTests(unittest.TestCase):
    def _analytics(self, root: Path) -> Path:
        analytics = root / "analytics"
        analytics.mkdir()
        (analytics / "data_quality_summary.json").write_text(json.dumps({"quality": 1}), encoding="utf-8")
        (analytics / "pipeline_status.json").write_text(json.dumps({"status": "succeeded"}), encoding="utf-8")
        return analytics

    def test_reports_missing_files_rather_than_failing(self):
        with tempfile.TemporaryDirectory() as directory:
            analytics = self._analytics(Path(directory))
            documents, missing = collect_reports(analytics)
            self.assertEqual({document["name"] for document in documents}, {"data_quality_summary", "pipeline_status"})
            self.assertIn("model_readiness_summary.json", missing)

    def test_dry_run_does_not_write(self):
        with tempfile.TemporaryDirectory() as directory:
            database = FakeDatabase()
            summary = publish_reports(self._analytics(Path(directory)), database, apply=False)
            self.assertFalse(summary["applied"])
            self.assertEqual(list(database["pipeline_reports"].find({})), [])

    def test_apply_replaces_the_previous_version_of_each_report(self):
        with tempfile.TemporaryDirectory() as directory:
            analytics = self._analytics(Path(directory))
            database = FakeDatabase()
            publish_reports(analytics, database, apply=True)
            (analytics / "pipeline_status.json").write_text(json.dumps({"status": "failed"}), encoding="utf-8")
            publish_reports(analytics, database, apply=True)

            rows = [row for row in database["pipeline_reports"].find({}) if row["name"] == "pipeline_status"]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["payload"]["status"], "failed")


if __name__ == "__main__":
    unittest.main()
