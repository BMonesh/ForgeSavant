"""Immutable, source-neutral landing store for analytics observations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Iterable
from urllib.parse import urlparse
from uuid import uuid4


SCHEMA_VERSION = "1.0"
OBSERVATION_KINDS = {"product_content", "retail_offer", "benchmark", "build_outcome"}
CATALOG_CATEGORIES = {
    "processors", "gpus", "motherboards", "ram", "storage", "power_supplies", "cabinets", "builds"
}
SENSITIVE_KEYS = {"password", "token", "secret", "authorization", "api_key", "apikey"}
RETAIL_AVAILABILITY = {"in_stock", "out_of_stock", "preorder", "unknown"}


@dataclass(frozen=True)
class IngestionResult:
    run_id: str
    received: int
    accepted: int
    duplicates: int
    quarantined: int
    manifest_path: str


def _utc(value: str) -> bool:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.tzinfo is not None
    except (AttributeError, TypeError, ValueError):
        return False


def _https(value: str) -> bool:
    try:
        parsed = urlparse(value)
        return parsed.scheme == "https" and bool(parsed.netloc)
    except (AttributeError, TypeError, ValueError):
        return False


def redact(value):
    if isinstance(value, dict):
        return {
            key: "[redacted]" if key.lower() in SENSITIVE_KEYS else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def validate_observation(record: dict) -> list[str]:
    errors = []
    if record.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")
    if record.get("observation_kind") not in OBSERVATION_KINDS:
        errors.append("observation_kind is invalid")
    if not re.fullmatch(r"[a-z][a-z0-9_]{1,63}", str(record.get("source", ""))):
        errors.append("source must be a lowercase machine identifier")
    if record.get("catalog_category") not in CATALOG_CATEGORIES:
        errors.append("catalog_category is invalid")
    if not str(record.get("source_product_id", "")).strip():
        errors.append("source_product_id is required")
    if not _utc(record.get("observed_at")):
        errors.append("observed_at must be an ISO-8601 timestamp with timezone")
    if not re.fullmatch(r"[a-f0-9]{64}", str(record.get("raw_sha256", ""))):
        errors.append("raw_sha256 must be a lowercase SHA-256 digest")
    if not isinstance(record.get("specifications"), dict):
        errors.append("specifications must be an object")
    if record.get("observation_kind") == "product_content":
        if not str(record.get("manufacturer", "")).strip():
            errors.append("manufacturer is required for product content")
        if not str(record.get("manufacturer_part_number", "")).strip():
            errors.append("manufacturer_part_number is required for product content")
    if record.get("observation_kind") == "retail_offer":
        if not str(record.get("catalog_name", "")).strip():
            errors.append("catalog_name is required for retail offers")
        if not str(record.get("manufacturer", "")).strip():
            errors.append("manufacturer is required for retail offers")
        if not str(record.get("manufacturer_part_number", "")).strip():
            errors.append("manufacturer_part_number is required for retail offers")
        price = record.get("price")
        if isinstance(price, bool) or not isinstance(price, (int, float)) or price <= 0:
            errors.append("price must be a positive number for retail offers")
        if not re.fullmatch(r"[A-Z]{3}", str(record.get("currency", ""))):
            errors.append("currency must be a three-letter uppercase code for retail offers")
        if record.get("availability") not in RETAIL_AVAILABILITY:
            errors.append("availability is invalid for retail offers")
        if not _https(record.get("source_record_url")):
            errors.append("source_record_url must be an HTTPS URL for retail offers")
    if record.get("observation_kind") == "build_outcome":
        if record.get("catalog_category") != "builds":
            errors.append("catalog_category must be builds for build outcomes")
        if record.get("event_type") not in {"build_saved", "build_updated"}:
            errors.append("event_type is invalid for build outcomes")
        for field in ("subject_hash", "build_hash"):
            if not re.fullmatch(r"[a-f0-9]{64}", str(record.get(field, ""))):
                errors.append(f"{field} must be a lowercase SHA-256 digest")
        if not isinstance(record.get("component_ids"), dict) or not record.get("component_ids"):
            errors.append("component_ids must be a non-empty object for build outcomes")
        build_total = record.get("build_total")
        if isinstance(build_total, bool) or not isinstance(build_total, (int, float)) or build_total < 0:
            errors.append("build_total must be a non-negative number for build outcomes")
        if record.get("currency") != "INR":
            errors.append("currency must be INR for build outcomes")
    if record.get("observation_kind") == "benchmark":
        for field in ("manufacturer", "manufacturer_part_number", "benchmark_name", "metric_name", "unit", "workload"):
            if not str(record.get(field, "")).strip():
                errors.append(f"{field} is required for benchmarks")
        metric_value = record.get("metric_value")
        if isinstance(metric_value, bool) or not isinstance(metric_value, (int, float)):
            errors.append("metric_value must be numeric for benchmarks")
        sample_count = record.get("sample_count")
        if isinstance(sample_count, bool) or not isinstance(sample_count, int) or sample_count < 1:
            errors.append("sample_count must be a positive integer for benchmarks")
        if not _https(record.get("source_record_url")):
            errors.append("source_record_url must be an HTTPS URL for benchmarks")
        if record.get("usage_basis") not in {"owner_provided", "licensed", "public_domain", "api_terms"}:
            errors.append("usage_basis is invalid for benchmarks")
    return errors


def observation_id(record: dict) -> str:
    identity = {
        "schema_version": record.get("schema_version"),
        "observation_kind": record.get("observation_kind"),
        "source": record.get("source"),
        "source_product_id": record.get("source_product_id"),
        "raw_sha256": record.get("raw_sha256"),
    }
    if record.get("observation_kind") != "product_content":
        identity["observed_at"] = record.get("observed_at")
    else:
        # A corrected catalog identity must be able to supersede an earlier
        # observation made from the same immutable source payload.
        identity["manufacturer_part_number"] = record.get("manufacturer_part_number")
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _write_jsonl(path: Path, rows: Iterable[dict]) -> str:
    content = "".join(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n" for row in rows)
    with path.open("x", encoding="utf-8", newline="\n") as output:
        output.write(content)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class ObservationStore:
    """Writes one immutable directory per ingestion run and never mutates MongoDB."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def _known_ids(self) -> set[str]:
        known = set()
        normalized = self.root / "normalized"
        if not normalized.exists():
            return known
        for path in normalized.rglob("observations.jsonl"):
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    value = json.loads(line).get("observation_id")
                    if value:
                        known.add(value)
        return known

    def ingest(self, source: str, observations: Iterable[dict], *, run_id: str | None = None) -> IngestionResult:
        received_at = datetime.now(timezone.utc)
        run_id = run_id or f"{received_at.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid4().hex[:8]}"
        if not re.fullmatch(r"[A-Za-z0-9._-]{8,96}", run_id):
            raise ValueError("run_id contains unsupported characters")

        raw_records = [redact(dict(record)) for record in observations]
        accepted = []
        quarantined = []
        duplicates = 0
        known_ids = self._known_ids()
        batch_ids = set()

        for index, record in enumerate(raw_records):
            errors = validate_observation(record)
            if record.get("source") != source:
                errors.append("record source does not match ingestion source")
            if errors:
                quarantined.append({"index": index, "errors": errors, "record": record})
                continue
            record_id = observation_id(record)
            if record_id in known_ids or record_id in batch_ids:
                duplicates += 1
                continue
            batch_ids.add(record_id)
            accepted.append({**record, "observation_id": record_id, "ingested_at": received_at.isoformat()})

        day = received_at.strftime("%Y-%m-%d")
        raw_dir = self.root / "raw" / source / day / run_id
        normalized_dir = self.root / "normalized" / day / run_id
        quarantine_dir = self.root / "quarantine" / day / run_id
        manifest_dir = self.root / "manifests"
        for directory in (raw_dir, normalized_dir, quarantine_dir, manifest_dir):
            directory.mkdir(parents=True, exist_ok=True)

        checksums = {
            "raw": _write_jsonl(raw_dir / "observations.jsonl", raw_records),
            "normalized": _write_jsonl(normalized_dir / "observations.jsonl", accepted),
            "quarantine": _write_jsonl(quarantine_dir / "observations.jsonl", quarantined),
        }
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "source": source,
            "received_at": received_at.isoformat(),
            "counts": {
                "received": len(raw_records),
                "accepted": len(accepted),
                "duplicates": duplicates,
                "quarantined": len(quarantined),
            },
            "checksums": checksums,
            "paths": {
                "raw": str(raw_dir / "observations.jsonl"),
                "normalized": str(normalized_dir / "observations.jsonl"),
                "quarantine": str(quarantine_dir / "observations.jsonl"),
            },
        }
        manifest_path = manifest_dir / f"{run_id}.json"
        with manifest_path.open("x", encoding="utf-8") as output:
            json.dump(manifest, output, indent=2, ensure_ascii=False)

        return IngestionResult(
            run_id=run_id,
            received=len(raw_records),
            accepted=len(accepted),
            duplicates=duplicates,
            quarantined=len(quarantined),
            manifest_path=str(manifest_path),
        )
