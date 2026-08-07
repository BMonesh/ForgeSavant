import sys
from pathlib import Path
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from ingest_benchmark_evidence import build_benchmark_observation  # noqa: E402


class BenchmarkEvidenceTests(unittest.TestCase):
    def test_builds_identity_linked_licensed_benchmark(self):
        identity = {"name": "Example CPU", "manufacturerPartNumber": "CPU-1"}
        row = build_benchmark_observation({
            "source": "owned_test_lab",
            "sourceRecordId": "run-1",
            "category": "processors",
            "manufacturer": "Example",
            "manufacturerPartNumber": "CPU-1",
            "benchmarkName": "Render test",
            "metricName": "completion_time",
            "metricValue": 42.5,
            "unit": "seconds",
            "workload": "Scene A, benchmark version 1",
            "settings": {"threads": "auto"},
            "sampleCount": 3,
            "sourceRecordUrl": "https://lab.example/results/run-1",
            "usageBasis": "owner_provided",
            "observedAt": "2026-07-23T00:00:00+00:00",
        }, identity)
        self.assertEqual(row["observation_kind"], "benchmark")
        self.assertEqual(row["specifications"]["sampleCount"], 3)

    def test_rejects_unlicensed_or_untraceable_benchmark(self):
        identity = {"name": "Example CPU", "manufacturerPartNumber": "CPU-1"}
        with self.assertRaisesRegex(ValueError, "source_record_url|usage_basis"):
            build_benchmark_observation({
                "source": "unknown_lab", "sourceRecordId": "run-1", "category": "processors",
                "manufacturer": "Example", "manufacturerPartNumber": "CPU-1",
                "benchmarkName": "Test", "metricName": "score", "metricValue": 1,
                "unit": "points", "workload": "Workload", "sampleCount": 1,
                "sourceRecordUrl": "", "usageBasis": "scraped",
                "observedAt": "2026-07-23T00:00:00+00:00",
            }, identity)


if __name__ == "__main__":
    unittest.main()
