import hashlib
import json
import sys
from pathlib import Path
import tempfile
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from observation_store import (  # noqa: E402
    MongoObservationStore,
    ObservationStore,
    jsonl_checksum,
    known_identities,
    observation_id,
    open_store,
)


class FakeBulkWriteError(Exception):
    def __init__(self, details):
        super().__init__("bulk write error")
        self.details = details


class FakeObjectId:
    """Stands in for bson.ObjectId, which is not JSON serializable."""

    _counter = 0

    def __init__(self):
        FakeObjectId._counter += 1
        self.value = f"fake-object-id-{FakeObjectId._counter}"

    def __repr__(self):
        return f"FakeObjectId({self.value})"


class FakeCollection:
    """Minimal stand-in that enforces the unique indexes the store relies on.

    Like pymongo, this stamps _id onto the caller's dicts in place. That
    mutation is load-bearing: reusing an inserted document for the manifest
    checksum would otherwise silently diverge from the filesystem backend.
    """

    def __init__(self):
        self.documents = []
        self.unique_keys = set()
        self.indexes = []

    def create_index(self, keys, unique=False, name=None):
        self.indexes.append({"keys": keys, "unique": unique, "name": name})
        if unique and isinstance(keys, str):
            self.unique_keys.add(keys)

    def _matches(self, document, query):
        for field, condition in query.items():
            if field == "$or":
                if not any(self._matches(document, clause) for clause in condition):
                    return False
                continue
            value = document.get(field)
            if isinstance(condition, dict) and "$in" in condition:
                if value not in condition["$in"]:
                    return False
            elif value != condition:
                return False
        return True

    def find(self, query=None, projection=None):
        """Honour both inclusion and exclusion projections, as pymongo does."""
        for document in self.documents:
            if not self._matches(document, query or {}):
                continue
            if not projection:
                yield dict(document)
                continue
            included = {key for key, value in projection.items() if value}
            excluded = {key for key, value in projection.items() if not value}
            yield {
                key: value for key, value in document.items()
                if (key in included if included else key not in excluded)
            }

    def insert_one(self, document):
        document.setdefault("_id", FakeObjectId())
        self.documents.append(dict(document))

    def replace_one(self, query, document, upsert=False):
        for index, existing in enumerate(self.documents):
            if self._matches(existing, query):
                self.documents[index] = dict(document)
                return
        if upsert:
            self.documents.append(dict(document))

    def insert_many(self, documents, ordered=True):
        write_errors = []
        for index, document in enumerate(documents):
            conflict = any(
                key in document and any(existing.get(key) == document[key] for existing in self.documents)
                for key in self.unique_keys
            )
            document.setdefault("_id", FakeObjectId())
            if conflict:
                write_errors.append({"index": index, "code": 11000, "errmsg": "duplicate key"})
                continue
            self.documents.append(dict(document))
        if write_errors:
            raise FakeBulkWriteError({"writeErrors": write_errors})


class FakeDatabase:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, FakeCollection())


def observation(**overrides):
    record = {
        "schema_version": "1.0",
        "observation_kind": "product_content",
        "source": "test_source",
        "source_product_id": "SKU-1",
        "catalog_category": "processors",
        "manufacturer": "Example",
        "manufacturer_part_number": "CPU-1",
        "observed_at": "2026-08-07T00:00:00+00:00",
        "raw_sha256": hashlib.sha256(b"payload").hexdigest(),
        "specifications": {"socket": "AM4"},
    }
    record.update(overrides)
    return record


def build_store(monkeypatched_errors=True):
    database = FakeDatabase()
    store = MongoObservationStore(database)
    store.ensure_indexes()
    return database, store


class MongoObservationStoreTests(unittest.TestCase):
    def setUp(self):
        # The store imports BulkWriteError lazily; point it at the fake.
        import observation_store

        self._pymongo = sys.modules.get("pymongo.errors")
        module = type(sys)("pymongo.errors")
        module.BulkWriteError = FakeBulkWriteError
        sys.modules["pymongo.errors"] = module
        self.observation_store = observation_store

    def tearDown(self):
        if self._pymongo is None:
            sys.modules.pop("pymongo.errors", None)
        else:
            sys.modules["pymongo.errors"] = self._pymongo

    def test_creates_the_unique_index_that_enforces_immutability(self):
        database, _ = build_store()
        names = {index["name"] for index in database["observation_records"].indexes}
        self.assertIn("observation_id_unique", names)

    def test_accepts_a_valid_batch_and_records_a_manifest(self):
        database, store = build_store()
        result = store.ingest("test_source", [observation()])
        self.assertEqual((result.received, result.accepted, result.duplicates, result.quarantined), (1, 1, 0, 0))
        manifest = next(iter(database["observation_manifests"].find({})))
        self.assertEqual(manifest["counts"]["accepted"], 1)
        self.assertEqual(manifest["storage"], "mongodb")

    def test_deduplicates_across_separate_runs(self):
        _, store = build_store()
        store.ingest("test_source", [observation()])
        second = store.ingest("test_source", [observation()])
        self.assertEqual((second.accepted, second.duplicates), (0, 1))

    def test_deduplicates_within_a_single_batch(self):
        _, store = build_store()
        result = store.ingest("test_source", [observation(), observation()])
        self.assertEqual((result.accepted, result.duplicates), (1, 1))

    def test_quarantines_invalid_records_without_blocking_valid_ones(self):
        database, store = build_store()
        result = store.ingest("test_source", [observation(), observation(catalog_category="invalid")])
        self.assertEqual((result.accepted, result.quarantined), (1, 1))
        entry = next(iter(database["observation_quarantine"].find({})))
        self.assertIn("catalog_category is invalid", entry["errors"])

    def test_rejects_a_record_whose_source_does_not_match(self):
        _, store = build_store()
        result = store.ingest("test_source", [observation(source="other_source")])
        self.assertEqual((result.accepted, result.quarantined), (0, 1))

    def test_redacts_sensitive_keys_before_landing_raw_payloads(self):
        database, store = build_store()
        store.ingest("test_source", [observation(specifications={"socket": "AM4", "api_key": "secret-value"})])
        raw = next(iter(database["observation_raw"].find({})))
        self.assertEqual(raw["record"]["specifications"]["api_key"], "[redacted]")

    def test_a_concurrent_insert_is_counted_as_a_duplicate(self):
        database, store = build_store()
        record = observation()
        # Simulate another runner winning the race between the dedup query and insert.
        store.ingest("test_source", [record])
        database["observation_records"].documents.append({"observation_id": "placeholder"})
        second = store.ingest("test_source", [record, observation(source_product_id="SKU-2")])
        self.assertEqual(second.accepted, 1)
        self.assertEqual(second.duplicates, 1)

    def test_read_observations_filters_by_kind(self):
        _, store = build_store()
        store.ingest("test_source", [observation()])
        self.assertEqual(len(list(store.read_observations(observation_kind="product_content"))), 1)
        self.assertEqual(len(list(store.read_observations(observation_kind="benchmark"))), 0)

    def test_rejects_an_unsafe_run_id(self):
        _, store = build_store()
        with self.assertRaisesRegex(ValueError, "unsupported characters"):
            store.ingest("test_source", [observation()], run_id="../escape")


def legacy_observation_id(record):
    """The superseded identity: the source payload alone, with no part number.

    Observations landed under this rule still exist in the lake, so both stores
    have to recognise them without reimplementing the rule for new records.
    """
    identity = {
        "schema_version": record.get("schema_version"),
        "observation_kind": record.get("observation_kind"),
        "source": record.get("source"),
        "source_product_id": record.get("source_product_id"),
        "raw_sha256": record.get("raw_sha256"),
    }
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class LegacyIdentityTests(unittest.TestCase):
    """Observations landed under the superseded id must not be re-accepted."""

    def setUp(self):
        module = type(sys)("pymongo.errors")
        module.BulkWriteError = FakeBulkWriteError
        sys.modules["pymongo.errors"] = module

    def tearDown(self):
        sys.modules.pop("pymongo.errors", None)

    def test_known_identities_covers_both_the_stored_and_current_id(self):
        record = observation()
        landed = {**record, "observation_id": legacy_observation_id(record)}
        identities = known_identities(landed)
        self.assertIn(legacy_observation_id(record), identities)
        self.assertIn(observation_id(record), identities)

    def test_mongo_treats_a_legacy_landed_observation_as_a_duplicate(self):
        database, store = build_store()
        record = observation()
        database["observation_records"].insert_one(
            {**record, "observation_id": legacy_observation_id(record)}
        )
        result = store.ingest("test_source", [record])
        self.assertEqual((result.accepted, result.duplicates), (0, 1))

    def test_filesystem_store_agrees(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ObservationStore(root)
            store.ingest("test_source", [observation()])
            landed = root / "normalized"
            path = next(landed.rglob("observations.jsonl"))
            record = json.loads(path.read_text(encoding="utf-8").strip())
            rewritten = {**record, "observation_id": legacy_observation_id(record)}
            path.write_text(json.dumps(rewritten, sort_keys=True) + "\n", encoding="utf-8")

            result = ObservationStore(root).ingest("test_source", [observation()])
            self.assertEqual((result.accepted, result.duplicates), (0, 1))

    def test_a_corrected_part_number_is_still_a_new_observation(self):
        """The correction the identity change exists for must not regress."""
        database, store = build_store()
        original = observation(manufacturer_part_number="OLD-MPN")
        database["observation_records"].insert_one(
            {**original, "observation_id": legacy_observation_id(original)}
        )
        corrected = observation(manufacturer_part_number="VERIFIED-MPN")
        result = store.ingest("test_source", [corrected])
        self.assertEqual((result.accepted, result.duplicates), (1, 0))


class BackendEquivalenceTests(unittest.TestCase):
    """The two backends must agree, or migrating changes what counts as evidence."""

    def setUp(self):
        module = type(sys)("pymongo.errors")
        module.BulkWriteError = FakeBulkWriteError
        sys.modules["pymongo.errors"] = module

    def tearDown(self):
        sys.modules.pop("pymongo.errors", None)

    def test_both_backends_agree_on_counts_and_checksums(self):
        batch = [
            observation(),
            observation(),
            observation(source_product_id="SKU-2", raw_sha256=hashlib.sha256(b"other").hexdigest()),
            observation(catalog_category="invalid"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            file_result = ObservationStore(Path(directory)).ingest("test_source", list(batch))
        _, mongo_store = build_store()
        mongo_result = mongo_store.ingest("test_source", list(batch))

        self.assertEqual(
            (file_result.received, file_result.accepted, file_result.duplicates, file_result.quarantined),
            (mongo_result.received, mongo_result.accepted, mongo_result.duplicates, mongo_result.quarantined),
        )

    def test_manifest_checksums_match_the_canonical_encoding(self):
        database, store = build_store()
        store.ingest("test_source", [observation()])
        manifest = next(iter(database["observation_manifests"].find({})))
        accepted = list(store.read_observations())
        self.assertEqual(manifest["checksums"]["normalized"], jsonl_checksum(accepted))
        self.assertEqual(manifest["checksums"]["quarantine"], jsonl_checksum([]))


class OpenStoreTests(unittest.TestCase):
    def test_returns_the_local_lake_without_a_uri(self):
        with tempfile.TemporaryDirectory() as directory:
            store = open_store(Path(directory), uri="")
            self.assertIsInstance(store, ObservationStore)

    def test_returns_the_mongo_store_with_a_uri(self):
        class Client:
            def __init__(self, database):
                self._database = database

            def get_default_database(self):
                return self._database

        with tempfile.TemporaryDirectory() as directory:
            store = open_store(
                Path(directory),
                uri="mongodb://example/forgesavant",
                client_factory=lambda uri: Client(FakeDatabase()),
            )
            self.assertIsInstance(store, MongoObservationStore)

    def test_rejects_a_uri_without_a_database_name(self):
        class Client:
            def get_default_database(self):
                return None

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "database name"):
                open_store(Path(directory), uri="mongodb://example", client_factory=lambda uri: Client())


if __name__ == "__main__":
    unittest.main()
