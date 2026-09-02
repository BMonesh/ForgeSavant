"""Land approved MongoDB price history as immutable retail-offer observations."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re

from dotenv import load_dotenv
from pymongo import MongoClient

from observation_store import open_store, SCHEMA_VERSION, validate_observation


BASE_DIR = Path(__file__).resolve().parent
COLLECTIONS = {
    "processors": "processors",
    "graphiccards": "gpus",
    "motherboards": "motherboards",
    "rams": "ram",
    "storages": "storage",
    "powersupplies": "power_supplies",
    "cabinets": "cabinets",
}


def _iso(value) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _source_id(value: object) -> str:
    source = re.sub(r"[^a-z0-9_]+", "_", str(value or "").casefold()).strip("_")
    if len(source) < 2:
        raise ValueError("price history source must produce a 2-64 character identifier")
    return source[:64]


def build_retail_observation(component: dict, history: dict, category: str) -> dict:
    identity = component.get("identity") or {}
    source_item_id = str(history.get("sourceItemId", "")).strip()
    if not source_item_id:
        raise ValueError("sourceItemId is required; seed price history is not retail evidence")
    observed_at = _iso(history.get("observedAt") or history.get("recordedAt"))
    source = _source_id(history.get("source"))
    immutable_offer = {
        "source": source,
        "sourceItemId": source_item_id,
        "catalogCategory": category,
        "catalogName": component.get("name", ""),
        "manufacturer": component.get("manufacturer", ""),
        "manufacturerPartNumber": identity.get("manufacturerPartNumber", ""),
        "price": history.get("price"),
        "currency": history.get("currency", ""),
        "availability": history.get("availability", ""),
        "sourceUrl": history.get("sourceUrl", ""),
        "observedAt": observed_at,
        "importChecksum": history.get("importChecksum", ""),
    }
    raw = json.dumps(immutable_offer, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    observation = {
        "schema_version": SCHEMA_VERSION,
        "observation_kind": "retail_offer",
        "source": source,
        "source_tier": "authorized_retailer",
        "source_product_id": source_item_id,
        "catalog_category": category,
        "catalog_name": str(component.get("name", "")).strip(),
        "manufacturer": str(component.get("manufacturer", "")).strip(),
        "manufacturer_part_number": str(identity.get("manufacturerPartNumber", "")).strip(),
        "price": history.get("price"),
        "currency": str(history.get("currency", "")).upper(),
        "availability": str(history.get("availability", "")).lower(),
        "source_record_url": str(history.get("sourceUrl", "")).strip(),
        "observed_at": observed_at,
        "import_checksum": str(history.get("importChecksum", "")).strip(),
        "specifications": {},
        "raw_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
    }
    errors = validate_observation(observation)
    if errors:
        raise ValueError("; ".join(errors))
    return observation


def snapshot_database(database, lake_dir: Path) -> dict:
    grouped: dict[str, list[dict]] = {}
    skipped = []
    scanned_components = 0
    scanned_history = 0
    for collection_name, category in COLLECTIONS.items():
        projection = {
            "name": 1, "manufacturer": 1, "identity.manufacturerPartNumber": 1,
            "priceHistory": 1,
        }
        for component in database[collection_name].find({}, projection):
            scanned_components += 1
            for index, history in enumerate(component.get("priceHistory") or []):
                scanned_history += 1
                try:
                    observation = build_retail_observation(component, history, category)
                    grouped.setdefault(observation["source"], []).append(observation)
                except (TypeError, ValueError) as error:
                    skipped.append({
                        "collection": collection_name,
                        "componentId": str(component.get("_id", "")),
                        "componentName": component.get("name", ""),
                        "historyIndex": index,
                        "reason": str(error),
                    })

    store = open_store(lake_dir)
    ingestions = []
    for source, observations in sorted(grouped.items()):
        result = store.ingest(source, observations)
        ingestions.append({
            "source": source,
            "received": result.received,
            "accepted": result.accepted,
            "duplicates": result.duplicates,
            "quarantined": result.quarantined,
            "runId": result.run_id,
        })
    return {
        "schemaVersion": "1.0",
        "snapshotAt": datetime.now(timezone.utc).isoformat(),
        "scannedComponents": scanned_components,
        "scannedPriceHistoryEntries": scanned_history,
        "eligibleOffers": sum(len(rows) for rows in grouped.values()),
        "accepted": sum(row["accepted"] for row in ingestions),
        "duplicates": sum(row["duplicates"] for row in ingestions),
        "quarantined": sum(row["quarantined"] for row in ingestions),
        "skipped": skipped,
        "ingestions": ingestions,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Snapshot approved MongoDB retail price history")
    parser.add_argument("--uri", help="MongoDB URI; defaults to URI from the ignored project .env")
    args = parser.parse_args()
    load_dotenv(BASE_DIR.parent / ".env")
    uri = args.uri or os.getenv("URI")
    if not uri:
        raise SystemExit("URI is required in .env or through --uri")
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    try:
        database = client.get_default_database(default="forgesavant")
        result = snapshot_database(database, BASE_DIR / "lake")
    finally:
        client.close()
    report_path = BASE_DIR / "analytics" / "retail_snapshot_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({key: result[key] for key in (
        "snapshotAt", "scannedComponents", "scannedPriceHistoryEntries",
        "eligibleOffers", "accepted", "duplicates", "quarantined",
    )}, indent=2))
    return 1 if result["quarantined"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
