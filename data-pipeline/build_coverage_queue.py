"""Build an actionable product-content coverage queue for the verified catalog."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from urllib.parse import urlparse

from observation_store import open_store


BASE_DIR = Path(__file__).resolve().parent
IDENTITY_FILES = {
    "processors": "processors.json",
    "gpus": "gpus.json",
    "motherboards": "motherboards.json",
    "ram": "ram.json",
    "storage": "storage.json",
    "power_supplies": "power-supplies.json",
    "cabinets": "cabinets.json",
}
CSV_FILES = {category: f"{category}_cleaned.csv" for category in IDENTITY_FILES}


def _key(value: object) -> str:
    return " ".join(str(value or "").casefold().split())


def _official_https(value: object) -> bool:
    parsed = urlparse(str(value or ""))
    return parsed.scheme == "https" and bool(parsed.netloc)


def _manufacturers(cleaned_dir: Path) -> dict[tuple[str, str], str]:
    values = {}
    for category, filename in CSV_FILES.items():
        path = cleaned_dir / filename
        if not path.exists():
            continue
        with path.open(encoding="utf-8-sig", newline="") as source:
            for row in csv.DictReader(source):
                values[(category, _key(row.get("name")))] = str(row.get("manufacturer", "")).strip()
    return values


def _observed_content(lake_dir: Path) -> dict[tuple[str, str], set[str]]:
    observed: dict[tuple[str, str], set[str]] = {}
    for row in open_store(lake_dir).read_observations(observation_kind="product_content"):
        key = (str(row.get("catalog_category", "")), _key(row.get("manufacturer_part_number")))
        if key[0] and key[1]:
            observed.setdefault(key, set()).add(str(row.get("source", "")))
    return observed


def _icecat_statuses(coverage_report: Path) -> dict[tuple[str, str], str]:
    if not coverage_report.exists():
        return {}
    report = json.loads(coverage_report.read_text(encoding="utf-8"))
    return {
        (str(row.get("component", "")), _key(row.get("manufacturer_part_number"))): str(row.get("status", ""))
        for row in report.get("records", [])
    }


def build_coverage_queue(
    identity_dir: Path,
    cleaned_dir: Path,
    lake_dir: Path,
    coverage_report: Path,
    output_json: Path,
    output_csv: Path,
) -> dict:
    manufacturers = _manufacturers(cleaned_dir)
    observed = _observed_content(lake_dir)
    icecat = _icecat_statuses(coverage_report)
    catalog = []
    category_totals = {}
    category_covered = {}

    for category, filename in IDENTITY_FILES.items():
        rows = json.loads((identity_dir / filename).read_text(encoding="utf-8-sig"))
        category_totals[category] = len(rows)
        for identity in rows:
            mpn = str(identity.get("manufacturerPartNumber", "")).strip()
            name = str(identity.get("name", "")).strip()
            sources = sorted(observed.get((category, _key(mpn)), set()))
            if sources:
                category_covered[category] = category_covered.get(category, 0) + 1
            catalog.append((category, identity, name, mpn, sources))

    records = []
    for category, identity, name, mpn, sources in catalog:
        source_url = str(identity.get("sourceUrl", "")).strip()
        latest_icecat = icecat.get((category, _key(mpn)), "not_audited")
        covered = bool(sources)
        official_ready = _official_https(source_url)
        if covered:
            status = "covered"
            gap_reason = ""
        elif official_ready:
            status = "manufacturer_ready"
            gap_reason = (
                "Open Icecat access is restricted; capture reviewed official manufacturer evidence."
                if latest_icecat == "restricted"
                else "No accepted product-content observation; capture reviewed official manufacturer evidence."
            )
        else:
            status = "source_missing"
            gap_reason = "A verified HTTPS manufacturer product page is required."

        total = category_totals[category]
        category_gap_rate = 1 - (category_covered.get(category, 0) / total if total else 0)
        priority = 0 if covered else round(
            50 + category_gap_rate * 30
            + (15 if latest_icecat == "restricted" else 10 if latest_icecat in {"not_found", "unavailable"} else 5)
            + (5 if official_ready else 0),
            2,
        )
        records.append({
            "category": category,
            "catalogName": name,
            "manufacturer": str(identity.get("manufacturer", "")).strip()
            or manufacturers.get((category, _key(identity.get("currentName") or name)), ""),
            "manufacturerPartNumber": mpn,
            "manufacturerSourceUrl": source_url,
            "contentObserved": covered,
            "contentSources": sources,
            "latestIcecatStatus": latest_icecat,
            "status": status,
            "gapReason": gap_reason,
            "priority": priority,
        })

    records.sort(key=lambda row: (-row["priority"], row["category"], row["catalogName"].casefold()))
    counts = {
        "verified": len(records),
        "covered": sum(row["contentObserved"] for row in records),
        "manufacturerReady": sum(row["status"] == "manufacturer_ready" for row in records),
        "sourceMissing": sum(row["status"] == "source_missing" for row in records),
    }
    payload = {
        "schemaVersion": "1.0",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "grain": "one verified catalog product",
        "counts": counts,
        "records": records,
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    with output_csv.open("w", encoding="utf-8", newline="") as target:
        fieldnames = [
            "priority", "status", "category", "catalogName", "manufacturer",
            "manufacturerPartNumber", "latestIcecatStatus", "contentObserved",
            "contentSources", "manufacturerSourceUrl", "gapReason",
        ]
        writer = csv.DictWriter(target, fieldnames=fieldnames)
        writer.writeheader()
        for row in records:
            writer.writerow({**row, "contentSources": "|".join(row["contentSources"])})
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the verified catalog product-content coverage queue")
    parser.parse_args()
    result = build_coverage_queue(
        BASE_DIR / "verified_identity",
        BASE_DIR / "cleaned_data",
        BASE_DIR / "lake",
        BASE_DIR / "icecat_coverage_report.json",
        BASE_DIR / "analytics" / "catalog_coverage_queue.json",
        BASE_DIR / "analytics" / "catalog_coverage_queue.csv",
    )
    print(json.dumps(result["counts"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
