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
- Ranked 58-product coverage queue with 44 uncovered products ready for reviewed official-manufacturer evidence and no missing official identity URLs.
- Immutable authorized-retailer offer snapshots; seed prices without retailer item IDs are rejected from price analytics.
- Retailer price collection from public product pages (added 2026-09-02, after the affiliate and licensed-feed routes were exhausted). Exact manufacturer-part-number matching against a published sitemap, schema.org offer parsing, robots.txt compliance, and ambiguity reported rather than guessed. Collected prices are an offer feed for signed administrator review; they cannot reach the catalog unreviewed.
- Explicitly opt-in product analytics that records only pseudonymous saved/updated build outcomes and deletes prior events when consent is revoked.
- Licensed benchmark ingestion contract with exact MPN matching, traceable HTTPS evidence, workload/settings context, sample count, and usage-basis validation.
- Liveness, readiness, health, CORS, Helmet, rate limiting, request-size limits, centralized errors, and production configuration validation.

## Data state

- 58/58 records have canonical identity keys, exact manufacturer part numbers, manufacturer evidence, and baseline price history.
- Zero normalized duplicates, orphaned retailer mappings, or orphaned saved-build references.
- 32/58 prices are live and 26/58 remain sample values, following the first collected retailer import on 2026-09-05. The 26 are products neither retailer lists; they stay labelled as planning data.
- 14/58 products currently have accepted Open Icecat content evidence: 6 GPUs, 3 motherboards, 3 RAM products, and 2 storage products.
- The warehouse retains 28 immutable observation versions while its current-product view resolves them to 14 latest logical observations; correction history no longer inflates coverage.
- The direct seed importer refuses to label data as live; only the signed partner-feed path may establish live price provenance.
- The price snapshot now admits 76 of 135 price-history entries as retailer evidence. The remainder are seed rows with no retailer item id and are still correctly refused.

## Data-science state

- Descriptive data-quality monitoring is ready and reproducible.
- Current Open Icecat product-content coverage is 24.14%, with 100% identity completeness, 92.86% GTIN coverage, 100% image coverage, and a 0% quarantine rate among current accepted observations.
- The warehouse now has separate product-content, retailer-price, benchmark, and consented-outcome views and Parquet extracts.
- The warehouse contains 24 public-domain Blender Open Data aggregate observations: exact matches for all 13 processors and 11 GPUs, with source version, median score, sample count, and query evidence retained.
- Supervised recommendations remain deliberately blocked because no user has opted in and a single benchmark family is not sufficient training evidence.
- India price forecasting is limited rather than blocked: the warehouse now holds 76 authorized price observations across 2 retailers, but only 2 collection dates. Temporal validation needs repeated, denser histories, so no forecast is published.
- Model-readiness gates now measure distinct retailer dates, independent benchmark sources, consented outcome dates, product coverage, and label availability before changing a modeling use from blocked.

## Release gate

`npm run verify` is the authoritative local gate. It currently passes 53 backend tests, 35 pipeline tests, 27 frontend tests, pipeline validation, identity verification, strict catalog-quality checks, lint, and the production build. Production startup additionally requires Node.js 20.19+, an explicit MongoDB URI, a non-placeholder 32+ character JWT secret, and HTTPS-only allowed origins. A separate high-entropy `ANALYTICS_PSEUDONYM_SECRET` is recommended for production outcome pseudonyms.

## External input still required

1. Live prices now have a working collection route: `npm run retail:scrape:apply` currently matches 30 of 58 catalog products at mdcomputers.in with 8 reported ambiguous and 20 not listed there. Those prices remain sample until an operator reviews and applies the feed through the signed admin import, so the 0/58 live figure above is unchanged until that review happens. An authorized partner feed or affiliate credentials would still be preferable, and additional retailers are needed for full coverage.
2. A second independent, licensed or otherwise authorized benchmark family is required before validating broader performance estimates; Blender Open Data is now the first.
3. Production hosting accounts, final HTTPS domains, MongoDB credentials, OAuth client configuration, and deployment authorization are required before publishing.

The application remains honest without those sources: planning values are
labeled, predictive claims stay blocked, and unavailable evidence is shown as
a measurable gap rather than fabricated data.
