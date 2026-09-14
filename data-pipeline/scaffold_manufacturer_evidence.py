"""Generate a ready-to-transcribe manufacturer evidence feed from the coverage queue.

This module deliberately does not crawl manufacturer websites, and it never
copies specification values out of the seed catalog. It only assembles the
identity scaffolding a reviewer needs -- category, verified name, manufacturer
part number, and the exact official URL from the identity manifest -- and leaves
every specification value null for a human to transcribe from the official page.

Pre-filling specifications from cleaned_data would launder sample planning data
into manufacturer evidence, which is precisely the boundary the ingestion path
exists to protect.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


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

# Columns that describe an offer or provenance rather than the product itself.
# These are never manufacturer evidence and must not appear in a capture sheet.
NON_SPECIFICATION_COLUMNS = {
    "name", "type", "manufacturer", "price", "image_url",
    "source", "source_url", "currency", "availability", "collected_at", "data_status",
}

# Fields the compatibility engine reads. A capture missing these still ingests,
# but it cannot improve a compatibility decision, so the scaffold flags them.
COMPATIBILITY_CRITICAL = {
    "processors": ("socket", "tdp"),
    "gpus": ("tdp",),
    "motherboards": ("socket", "chipset", "memory_type", "form_factor"),
    "ram": ("ram_type",),
    "storage": ("interface",),
    "power_supplies": ("wattage",),
    "cabinets": ("motherboard_support",),
}


def specification_fields(cleaned_dir: Path, category: str) -> list[str]:
    """Derive the expected specification keys from the cleaned catalog header."""
    path = cleaned_dir / f"{category}_cleaned.csv"
    if not path.exists():
        return list(COMPATIBILITY_CRITICAL.get(category, ()))
    with path.open(encoding="utf-8-sig", newline="") as source:
        header = next(csv.reader(source), [])
    fields = [column.strip() for column in header if column.strip() not in NON_SPECIFICATION_COLUMNS]
    for required in COMPATIBILITY_CRITICAL.get(category, ()):
        if required not in fields:
            fields.append(required)
    return fields


def load_identities(identity_dir: Path) -> dict[tuple[str, str], dict]:
    values = {}
    for category, filename in IDENTITY_FILES.items():
        for row in json.loads((identity_dir / filename).read_text(encoding="utf-8-sig")):
            key = (category, str(row.get("manufacturerPartNumber", "")).strip().casefold())
            values[key] = row
    return values


def build_scaffold(
    queue_path: Path,
    identity_dir: Path,
    cleaned_dir: Path,
    *,
    categories: set[str] | None = None,
    limit: int = 0,
) -> dict:
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    identities = load_identities(identity_dir)
    field_cache: dict[str, list[str]] = {}

    pending = [
        record for record in queue.get("records", [])
        if record.get("status") == "manufacturer_ready"
        and (not categories or record.get("category") in categories)
    ]
    pending.sort(key=lambda record: (
        -float(record.get("priority") or 0),
        record.get("category", ""),
        record.get("catalogName", ""),
    ))
    if limit > 0:
        pending = pending[:limit]

    records = []
    skipped = []
    for record in pending:
        category = str(record.get("category", "")).strip()
        mpn = str(record.get("manufacturerPartNumber", "")).strip()
        identity = identities.get((category, mpn.casefold()))
        if identity is None:
            skipped.append({
                "category": category,
                "manufacturerPartNumber": mpn,
                "reason": "not in the verified identity manifest",
            })
            continue

        # The ingester requires an exact match against the identity manifest, so the
        # URL is taken from there rather than from the queue projection.
        source_url = str(identity.get("sourceUrl", "")).strip()
        if not source_url.startswith("https://"):
            skipped.append({
                "category": category,
                "manufacturerPartNumber": mpn,
                "reason": "identity has no verified HTTPS source URL",
            })
            continue

        if category not in field_cache:
            field_cache[category] = specification_fields(cleaned_dir, category)

        records.append({
            "category": category,
            "catalogName": identity.get("name", ""),
            "manufacturer": str(record.get("manufacturer", "")).strip(),
            "manufacturerPartNumber": mpn,
            "officialSourceUrl": source_url,
            # Left empty on purpose: ingestion rejects the record until the reviewer
            # records when the official page was actually read.
            "observedAt": "",
            "documentRevision": "",
            "specifications": {field: None for field in field_cache[category]},
            "gtins": [],
            "imageUrl": "",
            "reviewNotes": "",
            "_capture": {
                "gapReason": record.get("gapReason", ""),
                "latestIcecatStatus": record.get("latestIcecatStatus", ""),
                "compatibilityCritical": list(COMPATIBILITY_CRITICAL.get(category, ())),
            },
        })

    return {
        "schemaVersion": "1.0",
        "generatedFrom": queue_path.name,
        "queueGeneratedAt": queue.get("generatedAt", ""),
        "instructions": (
            "Transcribe each value from the officialSourceUrl page, set observedAt to the "
            "UTC time you read it, then remove the _capture helper block and any "
            "specification left null. Records with an empty specifications object are rejected."
        ),
        "records": records,
        "skipped": skipped,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scaffold a manufacturer evidence feed for reviewer transcription",
    )
    parser.add_argument("--queue", type=Path, default=BASE_DIR / "analytics" / "catalog_coverage_queue.json")
    parser.add_argument("--output", type=Path, default=BASE_DIR / "manufacturer_evidence_feed.json")
    parser.add_argument("--component", action="append", default=[], help="Limit to a category; repeatable. Omit for all.")
    parser.add_argument("--limit", type=int, default=0, help="Only scaffold the N highest-priority products")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing output file")
    args = parser.parse_args()

    if not args.queue.exists():
        print(json.dumps({"error": f"{args.queue.name} not found; run npm run catalog:coverage first"}, indent=2))
        return 1

    categories = {value for value in args.component if value}
    unknown = categories - set(IDENTITY_FILES)
    if unknown:
        print(json.dumps({"error": f"unknown component(s): {sorted(unknown)}", "valid": sorted(IDENTITY_FILES)}, indent=2))
        return 1

    scaffold = build_scaffold(
        args.queue,
        BASE_DIR / "verified_identity",
        BASE_DIR / "cleaned_data",
        categories=categories or None,
        limit=args.limit,
    )

    if args.output.exists() and not args.force:
        print(json.dumps({
            "error": f"{args.output.name} already exists; pass --force to overwrite",
            "wouldScaffold": len(scaffold["records"]),
        }, indent=2))
        return 1

    args.output.write_text(json.dumps(scaffold, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "output": str(args.output),
        "scaffolded": len(scaffold["records"]),
        "skipped": scaffold["skipped"],
        "byCategory": {
            category: sum(1 for record in scaffold["records"] if record["category"] == category)
            for category in sorted({record["category"] for record in scaffold["records"]})
        },
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
