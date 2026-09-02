"""Publish generated analytics reports to MongoDB so any host can serve them.

The administrator data-health console reads these summaries from the API
server's local disk. Those files are gitignored, so a deployed service or a
scheduled runner never has them. Publishing them to the shared database lets a
pipeline run on one host update the console running on another.

Reports are derived summaries rather than observations, so the latest version of
each replaces the previous one. The immutable observation lake remains the
audit trail; nothing here is evidence.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
COLLECTION = "pipeline_reports"

# Exactly the files services/data-quality.service.js reads, keyed by the name
# the API asks for.
REPORTS = {
    "data_quality_summary": "data_quality_summary.json",
    "pipeline_status": "pipeline_status.json",
    "model_readiness_summary": "model_readiness_summary.json",
    "retail_snapshot_report": "retail_snapshot_report.json",
    "catalog_coverage_queue": "catalog_coverage_queue.json",
}


def collect_reports(analytics_dir: Path) -> tuple[list[dict], list[str]]:
    published_at = datetime.now(timezone.utc).isoformat()
    documents = []
    missing = []
    for name, filename in REPORTS.items():
        path = analytics_dir / filename
        if not path.exists():
            missing.append(filename)
            continue
        documents.append({
            "name": name,
            "filename": filename,
            "publishedAt": published_at,
            "payload": json.loads(path.read_text(encoding="utf-8")),
        })
    return documents, missing


def publish_reports(analytics_dir: Path, database, *, apply: bool = False) -> dict:
    documents, missing = collect_reports(analytics_dir)
    summary = {
        "analyticsDir": str(analytics_dir),
        "applied": bool(apply),
        "found": [document["name"] for document in documents],
        "missing": missing,
    }
    if not apply:
        summary["note"] = "Dry run. Re-run with --apply to publish."
        return summary

    collection = database[COLLECTION]
    collection.create_index("name", unique=True, name="report_name_unique")
    for document in documents:
        collection.replace_one({"name": document["name"]}, document, upsert=True)
    summary["published"] = len(documents)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish analytics reports to MongoDB")
    parser.add_argument("--analytics", type=Path, default=BASE_DIR / "analytics")
    parser.add_argument("--uri", help="MongoDB URI; defaults to OBSERVATION_STORE_URI, then URI")
    parser.add_argument("--apply", action="store_true", help="Write the reports; omit for a dry run")
    args = parser.parse_args()

    uri = args.uri or os.getenv("OBSERVATION_STORE_URI") or os.getenv("URI")
    if not uri:
        raise SystemExit("A MongoDB URI is required through --uri, OBSERVATION_STORE_URI, or URI")

    from pymongo import MongoClient

    database = MongoClient(uri, serverSelectionTimeoutMS=10000).get_default_database()
    if database is None:
        raise SystemExit("The MongoDB URI must include a database name")

    summary = publish_reports(args.analytics, database, apply=args.apply)
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 1 if summary["missing"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
