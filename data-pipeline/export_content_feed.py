"""Export accepted immutable product-content observations for admin review."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
LAKE_DIR = BASE_DIR / "lake" / "normalized"
OUTPUT_PATH = BASE_DIR / "authorized_product_content_feed.json"


def export_feed(lake_dir: Path = LAKE_DIR, output_path: Path = OUTPUT_PATH) -> dict:
    observations = {}
    for path in sorted(lake_dir.rglob("observations.jsonl")) if lake_dir.exists() else []:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
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
        "source": "open_icecat",
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "observations": sorted(observations.values(), key=lambda row: (row["catalog_category"], row["catalog_name"])),
    }
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


if __name__ == "__main__":
    result = export_feed()
    print(json.dumps({"output": str(OUTPUT_PATH), "observations": len(result["observations"])}, indent=2))
