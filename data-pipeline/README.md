# ForgeSavant Data Pipeline

Python-based data processing pipeline that cleans, validates, and imports hardware component data into the ForgeSavant MongoDB database. Source collection is connector-driven; the bundled catalog is planning data until a record includes verifiable retailer provenance.

## Pipeline Overview

```
raw_data/ (scraped CSVs)
    │
    ├── sync_retailer.py    → Exports approved API results for signed review
    ├── connectors/         → Source-specific authenticated API clients
    ├── scraper.py          → Legacy/local source inspection utilities
    │
    ├── data_cleaner.py     → Cleans, normalizes, deduplicates
    │
    ├── cleaned_data/       → Analysis-ready CSVs
    │
    ├── compatibility_engine.py  → Rule-based hardware validation
    │
    └── import_to_mongo.py  → Imports to MongoDB (matches Mongoose schemas)
```

## Setup

```bash
cd data-pipeline
pip install -r requirements.txt
```

## Usage

### 1. Clean Raw Data
```bash
# Clean all component types
python data_cleaner.py --all --stats

# Clean specific component
python data_cleaner.py --component processors --stats
```

Handles all seven catalog categories: processors, GPUs, motherboards, RAM, storage, power supplies, and cabinets. It normalizes inconsistent casing, duplicate entries, units (GHz/MHz, wattage, voltage), and missing values.

### 2. Validate Compatibility
```bash
# Run demo with sample builds
python compatibility_engine.py --demo

# Find compatible motherboards for a CPU
python compatibility_engine.py --find-compatible --cpu "AMD Ryzen 5 5600X"

# Validate a custom build from JSON
python compatibility_engine.py --check-build my_build.json
```

### 3. Import to MongoDB
```bash
# Dry run (validate without importing)
python import_to_mongo.py --dry-run --all

# Import to database
python import_to_mongo.py --all --uri mongodb://localhost:27017/forgesavant
```

### 4. Generate Scrape Report
```bash
python scraper.py --report
```

### 5. Sync official retailer offers

The first production connector targets the official Flipkart Affiliate API. It
uses authenticated JSON search—not storefront HTML scraping—and conservatively
matches offers to curated component names. Retail data updates price, image, and
provenance only; curated compatibility specifications remain authoritative.

Register for the affiliate program, review and accept the current API terms,
then set credentials in the shell (do not commit them):

```bash
export FLIPKART_AFFILIATE_ID=your_tracking_id
export FLIPKART_AFFILIATE_TOKEN=your_private_token
```

Preview matching without changing catalog CSV files:

```bash
python sync_retailer.py --component processors
```

Export conservative matches without mutating the curated CSV catalog:

```bash
python sync_retailer.py --component all --apply
```

This writes ignored `authorized_offer_feed.json`. Upload that file in the
administrator **Data import** screen, preview every match, and apply the signed
review. Live prices are written only through that audited application path;
`import_to_mongo.py` accepts seed/sample data only and preserves existing live
price provenance.

The generated report lists matched and unmatched catalog rows. A credentialed
API results become live only after signed administrator approval. Offline
fixtures can be used for dry-run reports but cannot be exported for application.
The API considers live records
stale after `CATALOG_FRESHNESS_HOURS` (24 hours by default).

Official references:

- https://affiliate.flipkart.com/api-docs/af_register.html
- https://affiliate.flipkart.com/api-docs/af_prod_ref.html
- https://affiliate.flipkart.com/api-docs/af_tou.html

### 6. Import an authorized partner feed

When a retailer provides CSV or JSON instead of an API, an administrator can
review and apply it at `/admin/offers`. Set the allowed operator emails on the
server, restart it, and sign in again so the session receives the admin claim:

```env
ADMIN_EMAILS=operator@example.com,second-operator@example.com
```

Download the template from the Data import page or use
`client/frontEnd/public/offer-feed-template.csv`. Required fields:

| Field | Rule |
|---|---|
| `name` | Retailer product title used for conservative catalog matching |
| `category` | CPU, GPU, motherboard, RAM, storage, power supply, or cabinet alias |
| `source_item_id` | Stable retailer SKU or external ID |
| `price` | Positive INR price |
| `currency` | `INR` |
| `availability` | `in_stock`, `out_of_stock`, `preorder`, or `unknown` |
| `source_url` | HTTPS product page URL |
| `image_url` | Optional HTTPS image URL |
| `observed_at` | ISO-8601 time when the retailer supplied or verified the value |

The preview validates every row and classifies it as ready, ambiguous,
unmatched, or rejected. Matching requires at least 80% meaningful title-token
coverage; equal top candidates are never applied automatically. Applying a
signed, 15-minute preview updates only verified rows and records an immutable
import checksum, operator email, category list, and counts. Replaying the exact
feed is idempotent. Curated hardware specifications are never overwritten.

### 7. Audit Open Icecat catalog coverage

Open Icecat is an optional product-content source for identities,
specifications, and images. It is not a retailer price source. Configure the
free account only in the ignored project `.env` file:

```env
ICECAT_USERNAME=your-open-icecat-username
ICECAT_PASSWORD="your-open-icecat-password"
ICECAT_LANGUAGE=EN
```

Run a bounded coverage audit against the verified manufacturer part numbers:

```bash
python data-pipeline/audit_icecat.py --component all
```

Use `--limit 5` for a connectivity smoke test. Bounded runs write
`icecat_coverage_smoke_report.json`, so they cannot replace the canonical
full-catalog coverage metrics. The generated ignored report
classifies every lookup as available, restricted, not found, or unavailable;
it never contains credentials. Add `--snapshot` only when immutable raw XML is
needed for inspectable analysis. The audit never modifies the application
catalog, because Open Icecat coverage depends on each manufacturer's content
syndication tier and must be measured before use.

Land accessible records in the source-neutral analytics store with:

```bash
python data-pipeline/audit_icecat.py --component all --ingest
```

Every run writes separate raw, normalized, quarantine, and manifest artifacts
under the ignored `data-pipeline/lake/` directory. Records are schema-validated,
credentials are recursively redacted, content observations are deduplicated by
source record and raw checksum, and every artifact receives a SHA-256 checksum.
This landing process does not update MongoDB; a later reviewed promotion step
will decide which normalized fields may enrich the application catalog.

### 8. Build analytical tables

Convert every immutable normalized observation and ingestion manifest into a
local DuckDB warehouse, compressed Parquet extracts, and a compact dashboard
summary:

```bash
npm run analytics:build
```

The derived, ignored `data-pipeline/analytics/` directory contains:

- `forgesavant.duckdb` with ingestion, observation, coverage, and quality views
- `parquet/*.parquet` for notebooks and model development
- `data_quality_summary.json` for the application monitoring API

This build is deterministic from the lake and can be recreated at any time. It
does not invent unavailable prices or treat product-content coverage as retail
coverage.

### 9. Build the catalog coverage work queue

Generate a ranked queue for every verified catalog identity:

```bash
npm run catalog:coverage
```

The ignored analytics directory receives `catalog_coverage_queue.json` for
application use and `catalog_coverage_queue.csv` for review. A product is
classified as covered, ready for official-manufacturer evidence capture, or
missing an official source. Priority is driven by category coverage and the
latest Open Icecat result.

### 10. Ingest reviewed manufacturer evidence

Copy `manufacturer_evidence_template.json` to the ignored
`manufacturer_evidence_feed.json`, replace the example with reviewed values
from the verified official product page, then run:

```bash
npm run catalog:manufacturer:ingest
```

The importer does not crawl arbitrary websites. It accepts evidence only when
the category, manufacturer part number, and HTTPS official URL exactly match
the verified identity manifest. Structured specifications are landed as
immutable product-content observations and remain separate from compatibility
rules until the existing reviewed promotion step is used.

### 11. Snapshot approved retailer price history

After an administrator has approved an authorized partner feed at
`/admin/offers`, land its MongoDB price history in the immutable analytical
store:

```bash
npm run retail:snapshot
```

For the bundled local MongoDB:

```bash
npm run retail:snapshot:local
```

Seed and planning prices are intentionally excluded. An eligible observation
must have a stable retailer item ID, positive price, ISO currency, supported
availability, HTTPS product URL, verified catalog MPN, and timestamp. Repeated
runs are idempotent; new timestamps form the price history needed for later
temporal analysis.

### 12. Run and monitor the complete analytical pipeline

From the repository root, run the authenticated Open Icecat audit, approved
retailer snapshot, immutable ingestion, DuckDB/Parquet rebuild, coverage work
queue, and model-readiness assessment as one job:

```bash
npm run pipeline:run
```

For a five-product connectivity smoke test, use `npm run pipeline:run:smoke`.
The runner prevents concurrent executions, rebuilds the model-readiness
assessment after the warehouse, and atomically writes
`analytics/pipeline_status.json` with stage timings and exit codes. Stored
diagnostic output is bounded and secret-redacted. A failed ingestion prevents
the analytical rebuild, so a previous valid warehouse is not presented as the
result of a failed collection.

Administrators can inspect the resulting KPIs at `/admin/data-quality`. The
dashboard distinguishes verified catalog size, source coverage, validation
pass rate, duplicate observations, quarantine rate, completeness, and
freshness. Known duplicates count as valid idempotent processing outcomes, not
as rejected data.

Scheduling belongs to the deployment environment. Schedule `npm run
pipeline:run` weekly and configure an alert on a non-zero exit code; do not put
credentials in the scheduler command itself. The job loads the ignored project
`.env` file.

### 13. Promote reviewed product content

Open Icecat content is deliberately separated from the compatibility catalog.
Export the latest logical observation for every accessible source product, then
perform an exact manufacturer-part-number review against Atlas:

```bash
npm run catalog:content:export
npm run catalog:content:preview
npm run catalog:content:apply
```

The apply step is idempotent and appends `productContentEvidence`; it does not
replace curated specifications, compatibility rules, or prices. Icecat's
source-reported regional SKU is preserved separately from the verified part
number used for the lookup. Administrators can run the same signed preview and
apply workflow from `/admin/content`.

Generate and execute the inspectable model-readiness notebook with:

```bash
npm run analytics:model-readiness
npm run analytics:notebook
```

The executed notebook is stored at `notebooks/model_readiness.ipynb`. It keeps
descriptive analysis separate from predictive claims and records the missing
labels and temporal evidence required before model fitting is defensible.

### 14. Capture consented outcomes and reviewed benchmarks

Product analytics is opt-in from the user's build-history page. Saving or
updating a build records only pseudonymous subject/build hashes, component IDs,
the catalog total, and model versions. It does not record email, name, IP,
user-agent, searches, or page views. Opting out deletes that subject's existing
events. Use a dedicated production secret:

```env
ANALYTICS_PSEUDONYM_SECRET=generate-a-separate-high-entropy-secret
```

Land consented events in the immutable lake with:

```bash
npm run outcomes:snapshot
```

For performance evidence, copy `benchmark_evidence_template.json` to the
ignored `benchmark_evidence_feed.json`, provide an owned or licensed result
linked to a verified manufacturer part number, then run:

```bash
npm run benchmarks:ingest
```

The benchmark contract requires a traceable HTTPS record, usage basis,
workload, metric, unit, sample count, settings, hardware context, and test time.
Unlicensed or identity-unmatched rows are rejected.

ForgeSavant also includes an exact-model connector for Blender Open Data:

```bash
npm run benchmarks:blender
```

It currently pins Blender Benchmark 5.1.1 for reproducibility, accepts only an
exact normalized GPU device name or an exact CPU model phrase, and preserves
the public aggregate's median score, sample count, query URL, version, and
collection time. Laptop, Ti, Super, and nearby model variants are not used as
substitutes. Blender states that anonymized Open Data benchmark results are
public-domain data:

Analytics retain every dated aggregate pull as a historical snapshot, while
`current_benchmark_observations` keeps only the newest record for each source
and exact product/version identity. The admin dashboard reports both counts so
repeated collection runs are not presented as independent benchmark samples.

- https://www.blender.org/news/introducing-blender-benchmark/

## Data Schema

The pipeline transforms flat CSV data into nested MongoDB documents matching the Mongoose schemas in `/models`:

| CSV Column | MongoDB Path |
|---|---|
| `cores` | `specifications.cores` |
| `base_clock` | `specifications.base_clock` |
| `socket` | `specifications.socket` |
| `memory_type` | `specifications.memory_type` |
| source fields | `provenance.*` |
| ... | `specifications.*` |

## Cleaning Rules

- **Manufacturers**: Canonical casing (`amd` → `AMD`, `nvidia` → `NVIDIA`)
- **Clock speeds**: `3.7 ghz` → `3.7 GHz`, `2460 mhz` → `2460 MHz`
- **Sockets**: `lga 1700` / `LGA1700` → `LGA 1700`
- **TDP**: `65 W` / `65w` → `65W`
- **Deduplication**: Same component from multiple vendors → keeps lowest price
