"""Validate and land licensed, identity-linked benchmark evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from ingest_manufacturer_evidence import load_verified_identities
from observation_store import ObservationStore, SCHEMA_VERSION, validate_observation


BASE_DIR = Path(__file__).resolve().parent


def build_benchmark_observation(evidence: dict, identity: dict) -> dict:
    immutable = {
        "source": evidence.get("source"),
        "sourceRecordId": evidence.get("sourceRecordId"),
        "category": evidence.get("category"),
        "catalogName": identity.get("name"),
        "manufacturer": evidence.get("manufacturer"),
        "manufacturerPartNumber": evidence.get("manufacturerPartNumber"),
        "benchmarkName": evidence.get("benchmarkName"),
        "metricName": evidence.get("metricName"),
        "metricValue": evidence.get("metricValue"),
        "unit": evidence.get("unit"),
        "workload": evidence.get("workload"),
        "resolution": evidence.get("resolution", ""),
        "settings": evidence.get("settings", {}),
        "sampleCount": evidence.get("sampleCount"),
        "hardwareContext": evidence.get("hardwareContext", {}),
        "sourceRecordUrl": evidence.get("sourceRecordUrl"),
        "usageBasis": evidence.get("usageBasis"),
        "sourceLicense": evidence.get("sourceLicense", ""),
        "observedAt": evidence.get("observedAt"),
    }
    raw = json.dumps(immutable, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    record = {
        "schema_version": SCHEMA_VERSION,
        "observation_kind": "benchmark",
        "source": immutable["source"],
        "source_tier": "reviewed_benchmark",
        "source_product_id": immutable["sourceRecordId"],
        "catalog_category": immutable["category"],
        "catalog_name": immutable["catalogName"],
        "manufacturer": immutable["manufacturer"],
        "manufacturer_part_number": immutable["manufacturerPartNumber"],
        "benchmark_name": immutable["benchmarkName"],
        "metric_name": immutable["metricName"],
        "metric_value": immutable["metricValue"],
        "unit": immutable["unit"],
        "workload": immutable["workload"],
        "sample_count": immutable["sampleCount"],
        "usage_basis": immutable["usageBasis"],
        "source_record_url": immutable["sourceRecordUrl"],
        "observed_at": immutable["observedAt"],
        "specifications": {
            "benchmarkName": immutable["benchmarkName"],
            "metricName": immutable["metricName"],
            "metricValue": immutable["metricValue"],
            "unit": immutable["unit"],
            "workload": immutable["workload"],
            "resolution": immutable["resolution"],
            "settings": immutable["settings"],
            "sampleCount": immutable["sampleCount"],
            "hardwareContext": immutable["hardwareContext"],
            "usageBasis": immutable["usageBasis"],
            "sourceLicense": immutable["sourceLicense"],
        },
        "raw_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
    }
    errors = validate_observation(record)
    if errors:
        raise ValueError("; ".join(errors))
    return record


def ingest_benchmarks(input_path: Path, identity_dir: Path, lake_dir: Path) -> dict:
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    if payload.get("schemaVersion") != "1.0" or not isinstance(payload.get("records"), list):
        raise ValueError("benchmark feed must use schemaVersion 1.0 and contain a records array")
    identities = load_verified_identities(identity_dir)
    grouped = {}
    rejected = []
    for index, evidence in enumerate(payload["records"]):
        key = (
            str(evidence.get("category", "")),
            str(evidence.get("manufacturerPartNumber", "")).strip().casefold(),
        )
        identity = identities.get(key)
        if identity is None:
            rejected.append({"index": index, "error": "manufacturer part number is not in the verified catalog"})
            continue
        try:
            record = build_benchmark_observation(evidence, identity)
            grouped.setdefault(record["source"], []).append(record)
        except ValueError as error:
            rejected.append({"index": index, "error": str(error)})
    ingestions = []
    store = ObservationStore(lake_dir)
    for source, records in sorted(grouped.items()):
        result = store.ingest(source, records)
        ingestions.append({
            "source": source, "accepted": result.accepted, "duplicates": result.duplicates,
            "quarantined": result.quarantined, "runId": result.run_id,
        })
    return {"schemaVersion": "1.0", "inputRecords": len(payload["records"]), "rejected": rejected, "ingestions": ingestions}


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest reviewed benchmark evidence")
    parser.add_argument("--input", type=Path, required=True)
    args = parser.parse_args()
    result = ingest_benchmarks(
        args.input,
        BASE_DIR / "verified_identity",
        BASE_DIR / "lake",
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 1 if result["rejected"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
