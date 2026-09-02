import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from run_pipeline import PipelineLock, run_pipeline, sanitize_output  # noqa: E402


class RunPipelineTests(unittest.TestCase):
    def test_runs_ingestion_before_analytics_and_records_success(self):
        with tempfile.TemporaryDirectory() as directory:
            pipeline_dir = Path(directory) / "data-pipeline"
            pipeline_dir.mkdir()
            calls = []

            def runner(command, **_kwargs):
                calls.append(command)
                return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

            status = run_pipeline(pipeline_dir=pipeline_dir, limit=5, command_runner=runner, publish_reports=False)

            self.assertEqual(status["status"], "succeeded")
            self.assertIn("audit_icecat.py", calls[0][1])
            self.assertEqual(calls[0][-2:], ["--limit", "5"])
            self.assertIn("snapshot_retail_offers.py", calls[1][1])
            self.assertIn("snapshot_build_outcomes.py", calls[2][1])
            self.assertIn("ingest_blender_benchmarks.py", calls[3][1])
            self.assertIn("build_analytics.py", calls[4][1])
            self.assertIn("build_coverage_queue.py", calls[5][1])
            self.assertIn("analyze_model_readiness.py", calls[6][1])
            saved = json.loads((pipeline_dir / "analytics" / "pipeline_status.json").read_text(encoding="utf-8"))
            self.assertEqual(
                [stage["status"] for stage in saved["stages"]],
                ["succeeded", "succeeded", "succeeded", "succeeded", "succeeded", "succeeded", "succeeded"],
            )
            self.assertFalse((pipeline_dir / "runtime" / "pipeline.lock").exists())

    def test_publishes_reports_only_when_a_shared_database_is_configured(self):
        def runner(command, **_kwargs):
            return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

        with tempfile.TemporaryDirectory() as directory:
            pipeline_dir = Path(directory) / "with-database"
            pipeline_dir.mkdir()
            status = run_pipeline(pipeline_dir=pipeline_dir, command_runner=runner, publish_reports=True)
            self.assertEqual(status["stages"][-1]["name"], "publish_reports")

        with tempfile.TemporaryDirectory() as directory:
            pipeline_dir = Path(directory) / "local-only"
            pipeline_dir.mkdir()
            status = run_pipeline(pipeline_dir=pipeline_dir, command_runner=runner, publish_reports=False)
            self.assertNotIn("publish_reports", [stage["name"] for stage in status["stages"]])

    def test_stops_after_failure_and_redacts_output(self):
        with tempfile.TemporaryDirectory() as directory:
            pipeline_dir = Path(directory) / "data-pipeline"
            pipeline_dir.mkdir()

            def runner(command, **_kwargs):
                return subprocess.CompletedProcess(command, 2, stdout="password=hunter2", stderr="bad")

            status = run_pipeline(pipeline_dir=pipeline_dir, command_runner=runner)

            self.assertEqual(status["status"], "failed")
            self.assertEqual(len(status["stages"]), 1)
            self.assertNotIn("hunter2", status["stages"][0]["outputTail"])

    def test_rejects_an_overlapping_run(self):
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "pipeline.lock"
            with PipelineLock(lock_path):
                with self.assertRaisesRegex(RuntimeError, "already running"):
                    with PipelineLock(lock_path):
                        pass

    def test_sanitizes_uri_credentials(self):
        output = sanitize_output("mongodb+srv://operator:private@example.test/db")
        self.assertNotIn("private", output)
        self.assertIn("[REDACTED]", output)


if __name__ == "__main__":
    unittest.main()
