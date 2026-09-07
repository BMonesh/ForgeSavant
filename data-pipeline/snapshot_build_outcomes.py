"""Land consented pseudonymous product outcomes from MongoDB into the immutable lake."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient

from observation_store import open_store, SCHEMA_VERSION


BASE_DIR = Path(__file__).resolve().parent


def _iso(value) -> str:
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def build_outcome_observation(event: dict) -> dict:
    immutable = {
        "eventId": str(event.get("_id", "")),
        "eventType": event.get("eventType"),
        "subjectHash": event.get("subjectHash"),
        "buildHash": event.get("buildHash"),
        "componentIds": {key: str(value) for key, value in (event.get("componentIds") or {}).items() if value},
        "buildTotal": event.get("buildTotal"),
        "currency": event.get("currency"),
        "compatibilityStatus": event.get("compatibilityStatus"),
        "compatibilityEngineVersion": event.get("compatibilityEngineVersion"),
        "analyticsModelVersion": event.get("analyticsModelVersion"),
        "occurredAt": _iso(event.get("occurredAt")),
    }
    raw = json.dumps(immutable, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return {
        "schema_version": SCHEMA_VERSION,
        "observation_kind": "build_outcome",
        "source": "forgesavant_app",
        "source_tier": "consented_product_analytics",
        "source_product_id": immutable["eventId"],
        "catalog_category": "builds",
        "catalog_name": "",
        "manufacturer": "",
        "manufacturer_part_number": "",
        "event_type": immutable["eventType"],
        "subject_hash": immutable["subjectHash"],
        "build_hash": immutable["buildHash"],
        "component_ids": immutable["componentIds"],
        "build_total": immutable["buildTotal"],
        "currency": immutable["currency"],
        "compatibility_status": immutable["compatibilityStatus"],
        "compatibility_engine_version": immutable["compatibilityEngineVersion"],
        "analytics_model_version": immutable["analyticsModelVersion"],
        "observed_at": immutable["occurredAt"],
        "specifications": {},
        "raw_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
    }


def snapshot_database(database, lake_dir: Path) -> dict:
    records = []
    rejected = []
    for event in database["analytics_events"].find({}):
        try:
            records.append(build_outcome_observation(event))
        except (TypeError, ValueError) as error:
            rejected.append({"eventId": str(event.get("_id", "")), "reason": str(error)})
    result = open_store(lake_dir).ingest("forgesavant_app", records) if records else None
    return {
        "schemaVersion": "1.0",
        "snapshotAt": datetime.now(timezone.utc).isoformat(),
        "received": len(records) + len(rejected),
        "eligible": len(records),
        "accepted": result.accepted if result else 0,
        "duplicates": result.duplicates if result else 0,
        "quarantined": result.quarantined if result else 0,
        "rejected": rejected,
        "runId": result.run_id if result else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Snapshot consented ForgeSavant build outcomes")
    parser.add_argument("--uri", help="MongoDB URI; defaults to URI from the ignored project .env")
    args = parser.parse_args()
    load_dotenv(BASE_DIR.parent / ".env")
    uri = args.uri or os.getenv("URI")
    if not uri:
        raise SystemExit("URI is required in .env or through --uri")
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    try:
        result = snapshot_database(client.get_default_database(default="forgesavant"), BASE_DIR / "lake")
    finally:
        client.close()
    report_path = BASE_DIR / "analytics" / "build_outcome_snapshot_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 1 if result["rejected"] or result["quarantined"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
