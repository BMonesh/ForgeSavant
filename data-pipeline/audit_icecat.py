"""Measure Open Icecat coverage for ForgeSavant's verified catalog identities."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re

from dotenv import load_dotenv

from connectors import IcecatAuthenticationError, OpenIcecatConnector
from observation_store import open_store


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
IDENTITY_DIR = BASE_DIR / "verified_identity"
REPORT_PATH = BASE_DIR / "icecat_coverage_report.json"
SMOKE_REPORT_PATH = BASE_DIR / "icecat_coverage_smoke_report.json"
SNAPSHOT_DIR = BASE_DIR / "raw_data" / "icecat"
LAKE_DIR = BASE_DIR / "lake"
COMPONENT_FILES = {
    "processors": "processors.json",
    "gpus": "gpus.json",
    "motherboards": "motherboards.json",
    "ram": "ram.json",
    "storage": "storage.json",
    "power_supplies": "power-supplies.json",
    "cabinets": "cabinets.json",
}


def manufacturer_for(entry: dict) -> str:
    if entry.get("manufacturer"):
        return str(entry["manufacturer"]).strip()
    return str(entry.get("name", "")).split(maxsplit=1)[0]


def safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_") or "unknown"


def load_entries(component: str) -> list[dict]:
    return json.loads((IDENTITY_DIR / COMPONENT_FILES[component]).read_text(encoding="utf-8-sig"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit Open Icecat coverage without changing the application catalog")
    parser.add_argument("--component", choices=[*COMPONENT_FILES, "all"], default="all")
    parser.add_argument("--limit", type=int, default=0, help="Maximum products to check; zero checks all selected products")
    parser.add_argument("--snapshot", action="store_true", help="Save immutable raw XML for accessible products")
    parser.add_argument("--ingest", action="store_true", help="Land available observations in the immutable analytics store")
    parser.add_argument("--report-path", type=Path, help="Optional report destination; bounded runs otherwise use the smoke report")
    args = parser.parse_args()

    report_path = args.report_path or (SMOKE_REPORT_PATH if args.limit > 0 else REPORT_PATH)
    if not report_path.is_absolute():
        report_path = PROJECT_DIR / report_path

    load_dotenv(PROJECT_DIR / ".env")
    username = os.getenv("ICECAT_USERNAME", "").strip()
    password = os.getenv("ICECAT_PASSWORD", "")
    language = os.getenv("ICECAT_LANGUAGE", "EN")
    if not username or not password:
        parser.error("Set ICECAT_USERNAME and ICECAT_PASSWORD in the project .env file")

    connector = OpenIcecatConnector(username, password, language=language)
    components = list(COMPONENT_FILES) if args.component == "all" else [args.component]
    work = [(component, entry) for component in components for entry in load_entries(component)]
    if args.limit > 0:
        work = work[:args.limit]

    started_at = datetime.now(timezone.utc)
    records = []
    for component, entry in work:
        mpn = str(entry.get("manufacturerPartNumber", "")).strip()
        manufacturer = manufacturer_for(entry)
        try:
            result = connector.lookup(mpn, manufacturer)
        except IcecatAuthenticationError as error:
            raise SystemExit(str(error)) from error

        record = {
            "component": component,
            "catalog_name": entry.get("name", ""),
            "manufacturer": manufacturer,
            "manufacturer_part_number": mpn,
            **result.to_dict(),
        }
        records.append(record)

        if args.snapshot and result.status == "available":
            run_path = SNAPSHOT_DIR / started_at.strftime("%Y%m%dT%H%M%S%fZ") / component
            run_path.mkdir(parents=True, exist_ok=True)
            with (run_path / f"{safe_filename(mpn)}.xml").open("x", encoding="utf-8") as snapshot:
                snapshot.write(result.raw_xml)

    counts = {}
    for record in records:
        counts[record["status"]] = counts.get(record["status"], 0) + 1
    report = {
        "schema_version": "1.0",
        "source": "open_icecat",
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "requested": len(work),
        "counts": counts,
        "records": records,
    }
    if args.ingest:
        observations = []
        for record in records:
            if record["status"] != "available":
                continue
            observation = dict(record["observation"])
            observation.update({
                "observation_kind": "product_content",
                "catalog_category": record["component"],
                "catalog_name": record["catalog_name"],
            })
            observations.append(observation)
        ingestion = open_store(LAKE_DIR).ingest("open_icecat", observations)
        report["ingestion"] = {
            "run_id": ingestion.run_id,
            "received": ingestion.received,
            "accepted": ingestion.accepted,
            "duplicates": ingestion.duplicates,
            "quarantined": ingestion.quarantined,
            "manifest_path": ingestion.manifest_path,
        }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({**{key: value for key, value in report.items() if key != "records"}, "report_path": str(report_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
