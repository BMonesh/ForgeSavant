"""Export accepted immutable product-content observations for admin review."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from observation_store import open_store


BASE_DIR = Path(__file__).resolve().parent
# The lake root, not lake/normalized: the store owns its internal layout now.
LAKE_DIR = BASE_DIR / "lake"
OUTPUT_PATH = BASE_DIR / "authorized_product_content_feed.json"


def export_feed(lake_dir: Path = LAKE_DIR, output_path: Path = OUTPUT_PATH) -> dict:
    observations = {}
    for record in open_store(lake_dir).read_observations(observation_kind="product_content"):
        logical_key = (
            record.get("source"),
            record.get("catalog_category"),
            record.get("source_product_id"),
        )
        previous = observations.get(logical_key)
        if previous is None or record.get("ingested_at", "") > previous.get("ingested_at", ""):
            observations[logical_key] = record
    payload = {
        "schema_version": "1.0",
        "source": "forgesavant_product_content",
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "source_counts": dict(sorted({
            source: sum(row.get("source") == source for row in observations.values())
            for source in {row.get("source", "unknown") for row in observations.values()}
        }.items())),
        "observations": sorted(observations.values(), key=lambda row: (row["catalog_category"], row["catalog_name"])),
    }
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


if __name__ == "__main__":
    result = export_feed()
    print(json.dumps({"output": str(OUTPUT_PATH), "observations": len(result["observations"])}, indent=2))
