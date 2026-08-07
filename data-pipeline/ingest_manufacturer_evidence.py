"""Validate and land reviewed official-manufacturer product evidence.

This module intentionally does not crawl manufacturer websites. It accepts a
reviewed structured capture, verifies it against the catalog's official URL and
manufacturer part number, then writes a source-neutral immutable observation.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlparse

from observation_store import ObservationStore, SCHEMA_VERSION


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


def _canonical_hash(value: dict) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _source_id(url: str) -> str:
    hostname = (urlparse(url).hostname or "").removeprefix("www.")
    value = re.sub(r"[^a-z0-9]+", "_", hostname.casefold()).strip("_")
    if not value:
        raise ValueError("officialSourceUrl must contain a valid HTTPS hostname")
    return f"manufacturer_{value}"[:64]


def load_verified_identities(identity_dir: Path) -> dict[tuple[str, str], dict]:
    values = {}
    for category, filename in IDENTITY_FILES.items():
        for row in json.loads((identity_dir / filename).read_text(encoding="utf-8-sig")):
            key = (category, str(row.get("manufacturerPartNumber", "")).strip().casefold())
            values[key] = row
    return values


def build_manufacturer_observation(evidence: dict, identity: dict) -> dict:
    category = str(evidence.get("category", "")).strip()
    mpn = str(evidence.get("manufacturerPartNumber", "")).strip()
    source_url = str(evidence.get("officialSourceUrl", "")).strip()
    verified_url = str(identity.get("sourceUrl", "")).strip()
    if source_url != verified_url:
        raise ValueError("officialSourceUrl must exactly match the verified identity manifest")
    parsed = urlparse(source_url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError("officialSourceUrl must be an HTTPS URL")
    if not str(evidence.get("manufacturer", "")).strip():
        raise ValueError("manufacturer is required")
    specifications = evidence.get("specifications")
    if not isinstance(specifications, dict) or not specifications:
        raise ValueError("specifications must be a non-empty object")
    observed_at = str(evidence.get("observedAt", "")).strip()
    try:
        parsed_time = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
        if parsed_time.tzinfo is None:
            raise ValueError
    except (TypeError, ValueError) as error:
        raise ValueError("observedAt must be an ISO-8601 timestamp with timezone") from error

    immutable_evidence = {
        "category": category,
        "catalogName": identity.get("name", ""),
        "manufacturer": evidence["manufacturer"],
        "manufacturerPartNumber": mpn,
        "officialSourceUrl": source_url,
        "observedAt": observed_at,
        "specifications": specifications,
        "imageUrl": evidence.get("imageUrl", ""),
        "documentRevision": evidence.get("documentRevision", ""),
        "reviewNotes": evidence.get("reviewNotes", ""),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "observation_kind": "product_content",
        "source": _source_id(source_url),
        "source_tier": "manufacturer",
        "source_product_id": mpn,
        "catalog_category": category,
        "catalog_name": identity.get("name", ""),
        "manufacturer": str(evidence["manufacturer"]).strip(),
        "manufacturer_part_number": mpn,
        "source_reported_part_number": mpn,
        "name": identity.get("name", ""),
        "category": category,
        "gtins": evidence.get("gtins", []),
        "specifications": specifications,
        "image_url": str(evidence.get("imageUrl", "")).strip(),
        "manufacturer_url": source_url,
        "source_record_url": source_url,
        "observed_at": observed_at,
        "raw_sha256": _canonical_hash(immutable_evidence),
        "document_revision": str(evidence.get("documentRevision", "")).strip(),
        "review_notes": str(evidence.get("reviewNotes", "")).strip(),
    }


def ingest_evidence(input_path: Path, identity_dir: Path, lake_dir: Path) -> dict:
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    if payload.get("schemaVersion") != "1.0" or not isinstance(payload.get("records"), list):
        raise ValueError("evidence feed must use schemaVersion 1.0 and contain a records array")
    identities = load_verified_identities(identity_dir)
    grouped: dict[str, list[dict]] = {}
    rejected = []
    for index, evidence in enumerate(payload["records"]):
        category = str(evidence.get("category", "")).strip()
        mpn = str(evidence.get("manufacturerPartNumber", "")).strip()
        identity = identities.get((category, mpn.casefold()))
        if identity is None:
            rejected.append({"index": index, "error": "manufacturer part number is not in the verified catalog"})
            continue
        try:
            observation = build_manufacturer_observation(evidence, identity)
            grouped.setdefault(observation["source"], []).append(observation)
        except ValueError as error:
            rejected.append({"index": index, "error": str(error)})

    results = []
    store = ObservationStore(lake_dir)
    for source, observations in sorted(grouped.items()):
        result = store.ingest(source, observations)
        results.append({
            "source": source,
            "runId": result.run_id,
            "received": result.received,
            "accepted": result.accepted,
            "duplicates": result.duplicates,
            "quarantined": result.quarantined,
        })
    return {
        "schemaVersion": "1.0",
        "processedAt": datetime.now(timezone.utc).isoformat(),
        "inputRecords": len(payload["records"]),
        "rejected": rejected,
        "ingestions": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest reviewed official manufacturer evidence")
    parser.add_argument("--input", type=Path, required=True, help="Reviewed manufacturer evidence JSON feed")
    args = parser.parse_args()
    result = ingest_evidence(
        args.input,
        BASE_DIR / "verified_identity",
        BASE_DIR / "lake",
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 1 if result["rejected"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
