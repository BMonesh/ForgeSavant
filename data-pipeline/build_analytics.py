"""Build reproducible DuckDB and Parquet analytics from immutable observations."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parent
LAKE_DIR = BASE_DIR / "lake"
ANALYTICS_DIR = BASE_DIR / "analytics"
COVERAGE_REPORT = BASE_DIR / "icecat_coverage_report.json"
IDENTITY_DIR = BASE_DIR / "verified_identity"


def _jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _identity_counts(identity_dir: Path) -> dict[str, int]:
    mapping = {
        "processors": "processors.json",
        "gpus": "gpus.json",
        "motherboards": "motherboards.json",
        "ram": "ram.json",
        "storage": "storage.json",
        "power_supplies": "power-supplies.json",
        "cabinets": "cabinets.json",
    }
    return {
        category: len(json.loads((identity_dir / filename).read_text(encoding="utf-8-sig")))
        for category, filename in mapping.items()
    }


def _load_runs(lake_dir: Path) -> list[dict]:
    manifests = lake_dir / "manifests"
    return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(manifests.glob("*.json"))] if manifests.exists() else []


def _load_observations(lake_dir: Path) -> list[dict]:
    normalized = lake_dir / "normalized"
    rows = []
    if normalized.exists():
        for path in sorted(normalized.rglob("observations.jsonl")):
            rows.extend(_jsonl(path))
    return rows


def build_analytics(lake_dir: Path, analytics_dir: Path, coverage_report: Path, identity_dir: Path) -> dict:
    analytics_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir = analytics_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    database_path = analytics_dir / "forgesavant.duckdb"
    temporary_database = analytics_dir / "forgesavant.next.duckdb"
    if temporary_database.exists():
        temporary_database.unlink()

    runs = _load_runs(lake_dir)
    observations = _load_observations(lake_dir)
    identity_counts = _identity_counts(identity_dir)
    coverage = json.loads(coverage_report.read_text(encoding="utf-8")) if coverage_report.exists() else {"records": []}
    coverage_records = coverage.get("records", [])

    connection = duckdb.connect(str(temporary_database))
    connection.execute("""
        CREATE TABLE ingestion_runs (
          run_id VARCHAR PRIMARY KEY, source VARCHAR, received_at TIMESTAMPTZ,
          received INTEGER, accepted INTEGER, duplicates INTEGER, quarantined INTEGER,
          raw_sha256 VARCHAR, normalized_sha256 VARCHAR, quarantine_sha256 VARCHAR
        )
    """)
    connection.execute("""
        CREATE TABLE catalog_observations (
          observation_id VARCHAR PRIMARY KEY, observation_kind VARCHAR, source VARCHAR,
          source_tier VARCHAR, source_product_id VARCHAR, catalog_category VARCHAR,
          catalog_name VARCHAR, manufacturer VARCHAR, manufacturer_part_number VARCHAR,
          source_reported_part_number VARCHAR, source_name VARCHAR, source_category VARCHAR,
          gtins_json JSON, specifications_json JSON,
          image_url VARCHAR, manufacturer_url VARCHAR, source_record_url VARCHAR,
          observed_at TIMESTAMPTZ, ingested_at TIMESTAMPTZ, raw_sha256 VARCHAR
        )
    """)
    connection.execute("""
        CREATE TABLE source_coverage (
          source VARCHAR, catalog_category VARCHAR, catalog_name VARCHAR,
          manufacturer VARCHAR, manufacturer_part_number VARCHAR, status VARCHAR, error VARCHAR
        )
    """)

    if runs:
        connection.executemany(
            "INSERT INTO ingestion_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(
                run["run_id"], run["source"], run["received_at"], run["counts"]["received"],
                run["counts"]["accepted"], run["counts"]["duplicates"], run["counts"]["quarantined"],
                run["checksums"]["raw"], run["checksums"]["normalized"], run["checksums"]["quarantine"],
            ) for run in runs],
        )
    if observations:
        connection.executemany(
            "INSERT INTO catalog_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(
                row["observation_id"], row["observation_kind"], row["source"], row.get("source_tier", ""),
                row["source_product_id"], row["catalog_category"], row.get("catalog_name", ""),
                row.get("manufacturer", ""), row.get("manufacturer_part_number", ""),
                row.get("source_reported_part_number", ""), row.get("name", ""),
                row.get("category", ""), json.dumps(row.get("gtins", []), ensure_ascii=False),
                json.dumps(row.get("specifications", {}), ensure_ascii=False), row.get("image_url", ""),
                row.get("manufacturer_url", ""), row.get("source_record_url", ""), row["observed_at"],
                row["ingested_at"], row["raw_sha256"],
            ) for row in observations],
        )
    if coverage_records:
        connection.executemany(
            "INSERT INTO source_coverage VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(
                coverage.get("source", "open_icecat"), row["component"], row["catalog_name"],
                row["manufacturer"], row["manufacturer_part_number"], row["status"], row.get("error", ""),
            ) for row in coverage_records],
        )

    connection.execute("""
        CREATE VIEW current_catalog_observations AS
        SELECT * EXCLUDE (logical_rank) FROM (
          SELECT *, row_number() OVER (
            PARTITION BY source, source_product_id
            ORDER BY ingested_at DESC, observation_id DESC
          ) AS logical_rank
          FROM catalog_observations
        ) WHERE logical_rank = 1
    """)
    connection.execute("""
        CREATE VIEW category_observation_quality AS
        SELECT catalog_category,
               count(*) AS observations,
               count(DISTINCT manufacturer_part_number) AS distinct_products,
               count(*) FILTER (WHERE manufacturer <> '' AND manufacturer_part_number <> '') AS complete_identity,
               count(*) FILTER (WHERE json_array_length(gtins_json) > 0) AS with_gtin,
               count(*) FILTER (WHERE image_url <> '') AS with_image
        FROM current_catalog_observations
        GROUP BY catalog_category
    """)
    connection.execute("""
        CREATE VIEW source_coverage_summary AS
        SELECT source, catalog_category, status, count(*) AS products
        FROM source_coverage
        GROUP BY source, catalog_category, status
    """)

    for table in ("ingestion_runs", "catalog_observations", "current_catalog_observations", "source_coverage"):
        target = (parquet_dir / f"{table}.parquet").resolve().as_posix().replace("'", "''")
        connection.execute(f"COPY {table} TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    category_rows = connection.execute("""
        SELECT catalog_category, observations, distinct_products, complete_identity, with_gtin, with_image
        FROM category_observation_quality ORDER BY catalog_category
    """).fetchall()
    run_totals = connection.execute("""
        SELECT coalesce(sum(received), 0), coalesce(sum(accepted), 0),
               coalesce(sum(duplicates), 0), coalesce(sum(quarantined), 0), max(received_at)
        FROM ingestion_runs
    """).fetchone()
    current_totals = connection.execute("""
        SELECT count(*),
               count(*) FILTER (WHERE manufacturer <> '' AND manufacturer_part_number <> ''),
               count(*) FILTER (WHERE json_array_length(gtins_json) > 0),
               count(*) FILTER (WHERE image_url <> '')
        FROM current_catalog_observations
    """).fetchone()
    connection.close()

    verified_total = sum(identity_counts.values())
    status_counts = {}
    available_by_category = {}
    for row in coverage_records:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1
        if row["status"] == "available":
            available_by_category[row["component"]] = available_by_category.get(row["component"], 0) + 1

    quality_by_category = {row[0]: {
        "observations": row[1], "distinctProducts": row[2], "completeIdentity": row[3],
        "withGtin": row[4], "withImage": row[5], "verifiedCatalogProducts": identity_counts.get(row[0], 0),
        "sourceCoverage": available_by_category.get(row[0], 0),
    } for row in category_rows}
    for category, count in identity_counts.items():
        quality_by_category.setdefault(category, {
            "observations": 0, "distinctProducts": 0, "completeIdentity": 0,
            "withGtin": 0, "withImage": 0, "verifiedCatalogProducts": count,
            "sourceCoverage": available_by_category.get(category, 0),
        })

    summary = {
        "schemaVersion": "1.0",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "sourcePath": str(lake_dir),
        "grain": "one immutable normalized source observation per source product content version",
        "catalog": {
            "verifiedProducts": verified_total,
            "observedProducts": current_totals[0],
            "openIcecatAvailable": status_counts.get("available", 0),
            "openIcecatRestricted": status_counts.get("restricted", 0),
            "openIcecatUnavailable": status_counts.get("unavailable", 0) + status_counts.get("not_found", 0),
        },
        "pipeline": {
            "runs": len(runs), "received": run_totals[0], "accepted": run_totals[1],
            "duplicates": run_totals[2], "quarantined": run_totals[3],
            "latestRunAt": run_totals[4].isoformat() if run_totals[4] else None,
        },
        "quality": {
            "identityCompletenessRate": (current_totals[1] / current_totals[0]) if current_totals[0] else None,
            "gtinCoverageRate": (current_totals[2] / current_totals[0]) if current_totals[0] else None,
            "imageCoverageRate": (current_totals[3] / current_totals[0]) if current_totals[0] else None,
            "quarantineRate": (run_totals[3] / run_totals[0]) if run_totals[0] else None,
        },
        "categories": dict(sorted(quality_by_category.items())),
        "caveats": [
            "Open Icecat is a product-content enrichment source, not an India retailer price source.",
            "Coverage reflects the latest bounded audit of the verified 58-product catalog.",
            "Current coverage uses the latest observation per source product; immutable correction history remains available separately.",
            "Predictive price or performance models are not considered validated by these observations.",
        ],
    }
    summary_path = analytics_dir / "data_quality_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    if database_path.exists():
        database_path.unlink()
    temporary_database.replace(database_path)
    return {**summary, "databasePath": str(database_path), "summaryPath": str(summary_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build ForgeSavant analytics tables from immutable observations")
    parser.parse_args()
    result = build_analytics(LAKE_DIR, ANALYTICS_DIR, COVERAGE_REPORT, IDENTITY_DIR)
    print(json.dumps({
        "generatedAt": result["generatedAt"], "catalog": result["catalog"],
        "pipeline": result["pipeline"], "quality": result["quality"],
        "databasePath": result["databasePath"], "summaryPath": result["summaryPath"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
