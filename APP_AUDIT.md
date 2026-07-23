# ForgeSavant application audit

Date: 2026-07-23

## Product boundary

ForgeSavant is a guided custom-PC planning application. It lets users inspect an exact retail-product catalog, build a configuration in dependency order, review server-generated compatibility evidence, and save or revise compatible builds.

It is not a retailer and does not claim real-time prices or measured benchmark results. Until an authorized offer feed is imported, prices are visibly labeled as sample planning values. CPU/GPU analytics are dimensionless or direct-spec planning indicators with low confidence, not benchmark scores or frame-rate forecasts.

## Implemented application

- One responsive visual system across landing, catalog evidence, builder, authentication, profile, and administrator import screens.
- Original standalone ForgeSavant F/S mark with transparent hero and featured-build assets.
- Nine-step builder with optional secondary storage, searchable/filterable choices, draft restoration, saved-build rehydration by stable IDs, and a persistent status dock.
- Server-authoritative socket, memory, storage, power, and case-fit checks using catalog IDs.
- Owner-scoped JWT authentication, verified Google sign-in, save/update/delete flows, and admin authorization.
- Exact manufacturer-verified identities and MPNs for all 58 catalog records across seven categories.
- Catalog evidence pages with identity, specifications, price state/history, and retailer mappings.
- Source-neutral CSV/JSON partner-feed preview/apply workflow with signed previews, manual resolutions, audit batches, and checksum idempotency.
- Authenticated Open Icecat collection, immutable correction history, exact-MPN review, and idempotent product-content promotion. Fourteen reviewed observations are currently attached to the live catalog.
- Python ETL that validates, normalizes, deduplicates, enriches, and upserts every category without overwriting verified live-price provenance with seed data.
- Reproducible DuckDB/Parquet warehouse, executed model-readiness notebook, monitored pipeline runs, and a protected administrator data-health console.
- Liveness, readiness, health, CORS, Helmet, rate limiting, request-size limits, centralized errors, and production configuration validation.

## Data state

- 58/58 records have canonical identity keys, exact manufacturer part numbers, manufacturer evidence, and baseline price history.
- Zero normalized duplicates, orphaned retailer mappings, or orphaned saved-build references.
- 0/58 prices are live and 58/58 are sample values. This is intentional until an authorized retailer feed is supplied.
- 14/58 products currently have accepted Open Icecat content evidence: 6 GPUs, 3 motherboards, 3 RAM products, and 2 storage products.
- The warehouse retains 28 immutable observation versions while its current-product view resolves them to 14 latest logical observations; correction history no longer inflates coverage.
- The direct seed importer refuses to label data as live; only the signed partner-feed path may establish live price provenance.

## Data-science state

- Descriptive data-quality monitoring is ready and reproducible.
- Current Open Icecat product-content coverage is 24.14%, with 100% identity completeness, 92.86% GTIN coverage, 100% image coverage, and a 0% quarantine rate among current accepted observations.
- Supervised recommendations remain deliberately blocked because no consented outcomes, preference labels, or benchmark targets exist.
- India price forecasting remains deliberately blocked because no authorized retailer price time series exists.
- The next defensible modeling inputs are repeated retailer snapshots, independently sourced benchmark observations at a declared workload, and consented product interaction outcomes.

## Release gate

`npm run verify` is the authoritative local gate. It runs backend tests, pipeline tests and validation, identity verification, strict catalog-quality checks, frontend tests, lint, and the production build. Production startup additionally requires Node.js 20.19+, an explicit MongoDB URI, a non-placeholder 32+ character JWT secret, and HTTPS-only allowed origins.

## External input still required

1. An authorized retailer/partner CSV or JSON offer feed (or working affiliate credentials) is needed to populate live prices. Flipkart registration being unavailable does not block the rest of the product.
2. A licensed or otherwise authorized benchmark source is required before training or presenting performance predictions.
3. Production hosting accounts, final HTTPS domains, MongoDB credentials, OAuth client configuration, and deployment authorization are required before publishing.

No benchmark dataset is required because the product no longer presents synthetic benchmark or FPS claims.
