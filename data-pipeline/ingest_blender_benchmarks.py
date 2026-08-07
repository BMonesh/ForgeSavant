"""Ingest exact-model public-domain Blender Open Data aggregates."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from connectors.blender_open_data import (
    BlenderOpenDataConnector,
    PUBLIC_DOMAIN_NOTICE,
    match_verified_device,
)
from ingest_benchmark_evidence import build_benchmark_observation
from observation_store import ObservationStore


BASE_DIR = Path(__file__).resolve().parent


def _manufacturer(identity: dict) -> str:
    if identity.get("manufacturer"):
        return str(identity["manufacturer"])
    name = str(identity.get("name", ""))
    if name.startswith("AMD "):
        return "AMD"
    if name.startswith("Intel "):
        return "Intel"
    return name.split(" ", 1)[0]


def ingest_blender(version: str, identity_dir: Path, lake_dir: Path, connector=None) -> dict:
    connector = connector or BlenderOpenDataConnector()
    collected_at = datetime.now(timezone.utc).isoformat()
    categories = {
        "processors": (
            json.loads((identity_dir / "processors.json").read_text(encoding="utf-8-sig")),
            connector.cpu_results(version),
            "CPU",
        ),
        "gpus": (
            json.loads((identity_dir / "gpus.json").read_text(encoding="utf-8-sig")),
            connector.gpu_results(version),
            "GPU",
        ),
    }
    observations = []
    unmatched = []
    matches = []
    for category, (identities, results, compute_type) in categories.items():
        for identity in identities:
            match = match_verified_device(identity, results, category)
            if match is None:
                unmatched.append({
                    "category": category,
                    "catalogName": identity.get("name"),
                    "manufacturerPartNumber": identity.get("manufacturerPartNumber"),
                })
                continue
            mpn = identity["manufacturerPartNumber"]
            evidence = {
                "source": "blender_open_data",
                "sourceRecordId": f"{version}:{category}:{mpn}",
                "category": category,
                "manufacturer": _manufacturer(identity),
                "manufacturerPartNumber": mpn,
                "benchmarkName": f"Blender Open Data {version}",
                "metricName": "median_score",
                "metricValue": match.median_score,
                "unit": "Blender Benchmark points",
                "workload": f"Official Blender Benchmark {version} scenes, aggregated by device name",
                "resolution": "",
                "settings": {"computeType": compute_type, "aggregation": "median"},
                "sampleCount": match.benchmark_count,
                "hardwareContext": {
                    "deviceName": match.device_name,
                    "scope": "Public aggregate across anonymized submissions",
                },
                "sourceRecordUrl": match.query_url,
                "usageBasis": "public_domain",
                "sourceLicense": f"Blender Open Data public-domain notice: {PUBLIC_DOMAIN_NOTICE}",
                "observedAt": collected_at,
            }
            observations.append(build_benchmark_observation(evidence, identity))
            matches.append({
                "category": category,
                "catalogName": identity.get("name"),
                "deviceName": match.device_name,
                "medianScore": match.median_score,
                "sampleCount": match.benchmark_count,
            })

    result = ObservationStore(lake_dir).ingest("blender_open_data", observations) if observations else None
    return {
        "schemaVersion": "1.0",
        "blenderVersion": version,
        "collectedAt": collected_at,
        "matched": len(matches),
        "unmatched": unmatched,
        "accepted": result.accepted if result else 0,
        "duplicates": result.duplicates if result else 0,
        "quarantined": result.quarantined if result else 0,
        "runId": result.run_id if result else None,
        "matches": matches,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest public-domain Blender Open Data aggregates")
    parser.add_argument("--version", default="5.1.1", help="Exact Blender benchmark version")
    args = parser.parse_args()
    result = ingest_blender(
        args.version,
        BASE_DIR / "verified_identity",
        BASE_DIR / "lake",
    )
    report_path = BASE_DIR / "analytics" / "blender_benchmark_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({key: result[key] for key in (
        "blenderVersion", "collectedAt", "matched", "unmatched",
        "accepted", "duplicates", "quarantined", "runId",
    )}, indent=2, ensure_ascii=False))
    return 1 if result["unmatched"] or result["quarantined"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
