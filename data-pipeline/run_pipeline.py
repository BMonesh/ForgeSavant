"""Run ForgeSavant ingestion and analytics as one observable, non-overlapping job."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Callable


BASE_DIR = Path(__file__).resolve().parent
SECRET_PATTERNS = (
    (re.compile(r"(mongodb(?:\+srv)?://[^:\s]+:)[^@\s]+", re.IGNORECASE), r"\1[REDACTED]"),
    (re.compile(r"((?:password|token|secret|api[_-]?key)\s*[=:]\s*)\S+", re.IGNORECASE), r"\1[REDACTED]"),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_output(value: str, limit: int = 4000) -> str:
    sanitized = value or ""
    for pattern, replacement in SECRET_PATTERNS:
        sanitized = pattern.sub(replacement, sanitized)
    return sanitized[-limit:]


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


class PipelineLock:
    def __init__(self, path: Path):
        self.path = path
        self.acquired = False

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as error:
            raise RuntimeError(f"Pipeline is already running (lock: {self.path})") from error
        with os.fdopen(descriptor, "w", encoding="utf-8") as lock:
            json.dump({"pid": os.getpid(), "startedAt": utc_now()}, lock)
        self.acquired = True
        return self

    def __exit__(self, _error_type, _error, _traceback):
        if self.acquired:
            self.path.unlink(missing_ok=True)


def run_pipeline(
    pipeline_dir: Path = BASE_DIR,
    limit: int = 0,
    command_runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    publish_reports: bool | None = None,
) -> dict:
    # Resolved once, here, so the stage list is a function of the arguments
    # rather than of whatever happens to be in the environment.
    if publish_reports is None:
        publish_reports = bool(os.getenv("OBSERVATION_STORE_URI") or os.getenv("URI"))
    status_path = pipeline_dir / "analytics" / "pipeline_status.json"
    lock_path = pipeline_dir / "runtime" / "pipeline.lock"
    status = {
        "schemaVersion": "1.0",
        "status": "running",
        "startedAt": utc_now(),
        "completedAt": None,
        "stages": [],
    }

    commands = [
        ("open_icecat_ingestion", [sys.executable, str(pipeline_dir / "audit_icecat.py"), "--component", "all", "--ingest"]),
        ("retail_offer_snapshot", [sys.executable, str(pipeline_dir / "snapshot_retail_offers.py")]),
        ("build_outcome_snapshot", [sys.executable, str(pipeline_dir / "snapshot_build_outcomes.py")]),
        ("blender_open_data_benchmarks", [sys.executable, str(pipeline_dir / "ingest_blender_benchmarks.py")]),
        ("analytics_build", [sys.executable, str(pipeline_dir / "build_analytics.py")]),
        ("coverage_queue", [sys.executable, str(pipeline_dir / "build_coverage_queue.py")]),
        ("model_readiness", [sys.executable, str(pipeline_dir / "analyze_model_readiness.py")]),
    ]
    if limit > 0:
        commands[0][1].extend(["--limit", str(limit)])

    # Only meaningful once a shared database is configured. A run on a host that
    # keeps its lake locally has nothing to publish to other hosts.
    if publish_reports:
        commands.append(
            ("publish_reports", [sys.executable, str(pipeline_dir / "publish_reports.py"), "--apply"])
        )

    with PipelineLock(lock_path):
        write_json_atomic(status_path, status)
        try:
            for name, command in commands:
                started_at = utc_now()
                started_clock = time.monotonic()
                result = command_runner(
                    command,
                    cwd=str(pipeline_dir.parent),
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    check=False,
                )
                stage = {
                    "name": name,
                    "status": "succeeded" if result.returncode == 0 else "failed",
                    "startedAt": started_at,
                    "completedAt": utc_now(),
                    "durationSeconds": round(time.monotonic() - started_clock, 3),
                    "exitCode": result.returncode,
                    "outputTail": sanitize_output("\n".join(part for part in (result.stdout, result.stderr) if part)),
                }
                status["stages"].append(stage)
                write_json_atomic(status_path, status)
                if result.returncode != 0:
                    raise RuntimeError(f"Stage {name} failed with exit code {result.returncode}")
            status["status"] = "succeeded"
        except Exception as error:
            status["status"] = "failed"
            status["error"] = sanitize_output(str(error), 500)
        finally:
            status["completedAt"] = utc_now()
            write_json_atomic(status_path, status)

    return status


def main() -> int:
    parser = argparse.ArgumentParser(description="Run catalog, retail-offer, and analytics stages with locking and health reporting")
    parser.add_argument("--limit", type=int, default=0, help="Limit Open Icecat lookups; zero runs the full catalog")
    args = parser.parse_args()
    status = run_pipeline(limit=args.limit)
    print(json.dumps(status, indent=2, ensure_ascii=False))
    return 0 if status["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
