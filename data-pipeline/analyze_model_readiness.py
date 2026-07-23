"""Produce an evidence-backed assessment of whether ForgeSavant data can support ML."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics

import duckdb


BASE_DIR = Path(__file__).resolve().parent


def analyze(database_path: Path, summary_path: Path) -> dict:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    connection = duckdb.connect(str(database_path), read_only=True)
    totals = connection.execute("""
        SELECT count(*) AS rows,
               count(DISTINCT observation_id) AS observation_ids,
               count(DISTINCT manufacturer_part_number) AS products,
               count(*) FILTER (WHERE observed_at > now()) AS future_observations
        FROM catalog_observations
    """).fetchone()
    current_totals = connection.execute("""
        SELECT count(*), count(*) FILTER (WHERE observed_at > now())
        FROM current_catalog_observations
    """).fetchone()
    categories = connection.execute("""
        SELECT catalog_category, count(DISTINCT manufacturer_part_number) AS observed_products
        FROM current_catalog_observations GROUP BY catalog_category ORDER BY catalog_category
    """).fetchall()
    specifications = [json.loads(row[0] or "{}") for row in connection.execute(
        "SELECT CAST(specifications_json AS VARCHAR) FROM current_catalog_observations"
    ).fetchall()]
    run_dates = connection.execute(
        "SELECT count(DISTINCT CAST(received_at AS DATE)) FROM ingestion_runs"
    ).fetchone()[0]
    connection.close()

    specification_counts = [len(value) for value in specifications]
    verified = summary["catalog"]["verifiedProducts"]
    observed = summary["catalog"]["observedProducts"]
    category_rows = []
    observed_by_category = dict(categories)
    for category, values in sorted(summary["categories"].items()):
        category_observed = observed_by_category.get(category, 0)
        category_verified = values["verifiedCatalogProducts"]
        category_rows.append({
            "category": category,
            "observedProducts": category_observed,
            "verifiedProducts": category_verified,
            "coverageRate": category_observed / category_verified if category_verified else None,
        })

    return {
        "schemaVersion": "1.0",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "dataset": {
            "grain": summary["grain"],
            "observations": totals[0],
            "distinctObservationIds": totals[1],
            "distinctProducts": current_totals[0],
            "verifiedProducts": verified,
            "catalogCoverageRate": observed / verified if verified else None,
            "ingestionRuns": summary["pipeline"]["runs"],
            "distinctIngestionDates": run_dates,
            "futureObservations": current_totals[1],
            "medianSpecificationFields": statistics.median(specification_counts) if specification_counts else 0,
            "distinctSpecificationFields": len({key for value in specifications for key in value}),
        },
        "categories": category_rows,
        "checks": {
            "uniqueObservationIds": totals[0] == totals[1],
            "noFutureObservations": totals[3] == 0,
            "allCategoriesObserved": all(row["observedProducts"] > 0 for row in category_rows),
            "supervisedOutcomeLabelsPresent": False,
            "temporalHistoryAtLeastEightDates": run_dates >= 8,
        },
        "uses": [
            {"use": "Descriptive data-quality monitoring", "status": "ready", "reason": "Validated manifests, immutable observations, and reconciled quality metrics are available."},
            {"use": "Product-content enrichment pilot", "status": "limited", "reason": f"Only {observed} of {verified} verified products have accepted Open Icecat content, with entire categories uncovered."},
            {"use": "Supervised build recommendation model", "status": "blocked", "reason": "There are no observed user outcomes, benchmark targets, or preference labels for supervised learning."},
            {"use": "India price prediction or forecasting", "status": "blocked", "reason": "Open Icecat supplies product content, not retailer prices, and the warehouse has no product-price time series."},
        ],
        "requiredNextEvidence": [
            "Authorized retailer offer snapshots with product identity, INR price, availability, retailer, and observation time.",
            "Independent performance benchmark observations at a declared workload, resolution, settings, and test date.",
            "Consented product interaction and saved-build outcomes before any personalized recommendation model.",
            "Repeated collection dates and leakage-safe train/validation/test splits before temporal evaluation.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Assess model readiness from the analytical warehouse")
    parser.add_argument("--database", type=Path, default=BASE_DIR / "analytics" / "forgesavant.duckdb")
    parser.add_argument("--summary", type=Path, default=BASE_DIR / "analytics" / "data_quality_summary.json")
    parser.add_argument("--output", type=Path, default=BASE_DIR / "analytics" / "model_readiness_summary.json")
    args = parser.parse_args()
    result = analyze(args.database, args.summary)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
