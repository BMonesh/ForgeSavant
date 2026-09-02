# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

ForgeSavant is a guided custom-PC planning app: a React/Vite frontend, an Express + Mongoose API, and a Python ETL/analytics pipeline over a MongoDB catalog of 58 exact retail products across seven hardware categories.

## Commands

All npm commands run from the repository root unless noted. `*:local` variants override `URI` to the Dockerized `mongodb://127.0.0.1:27018/forgesavant` instance and ignore `.env`.

```bash
npm run setup:local     # one-time: start Docker Mongo, seed, migrate identities, enrich, audit quality
npm run dev:local       # start local DB + API (nodemon) + Vite dev server together
npm run verify          # THE release gate: backend + pipeline tests, catalog audits, frontend test/lint/build
npm run verify:local    # same gate against the local Docker database
```

Tests:

```bash
npm test                                        # backend; the script lists every test file explicitly
node --test test/compatibility.service.test.js  # single backend test file
npm --prefix client/frontEnd test               # vitest (jsdom)
npm --prefix client/frontEnd run test -- Build  # single frontend test by name filter
python -m unittest discover -s data-pipeline/tests -v
python -m unittest data-pipeline.tests.test_run_pipeline -v
npm --prefix client/frontEnd run lint           # eslint, --max-warnings 0
```

When adding a backend test file you **must** append it to the `test` script in `package.json` — `node --test` is given an explicit file list, so a new file is otherwise silently skipped.

Catalog and pipeline maintenance (see `README.md` for the full list): `catalog:audit` / `catalog:migrate`, `catalog:identity:audit` / `catalog:identity:apply`, `catalog:quality:strict`, `catalog:content:preview` / `catalog:content:apply`, `pipeline:run`, `analytics:build`. The audit/preview form never writes; `--apply` writes and takes a backup first.

Manufacturer content coverage runs `catalog:coverage` → `catalog:manufacturer:scaffold` → transcribe → `catalog:manufacturer:ingest`.

## Collection policy — read before adding a data source

The owner lifted the previous no-scraping rule on 2026-09-02, after the affiliate and licensed-feed routes were exhausted. Retailer **price** pages may now be read by `connectors/retailer_scraper.py`. Everything else is unchanged, and the limits below are load-bearing rather than stylistic:

- **Manufacturer specifications are still never crawled.** `ingest_manufacturer_evidence.py` and `scaffold_manufacturer_evidence.py` say so in their docstrings and still mean it. Specs come from human transcription of official pages; the scraper only reads prices.
- **Exact matching only.** Attribution requires a verified MPN resolving to exactly one URL, compared on whole token runs via `slug_identifiers()`. Never reintroduce substring matching — `BX8071513600K` is a prefix of `BX8071513600KF`, a different CPU. Multiple candidates go to `Discovery.ambiguous` for a human, never a guess.
- **Prices come from schema.org JSON-LD**, not CSS selectors. A page without `Product` markup raises `RetailerPageUnavailable`; it must never degrade to a zero or partial price.
- **Review is still mandatory.** The scraper writes an offer feed for the signed admin preview/apply path. It never touches MongoDB and never edits compatibility specs. Do not add a path that lets collected prices reach the catalog unreviewed.
- **`amazon.in` does no discovery.** `AmazonIndiaAdapter` only visits `/dp/{asin}` for operator-supplied ASINs. Amazon's Conditions of Use forbid automated data gathering regardless of robots.txt, so it must not gain search or crawl behaviour.
- **robots.txt is honoured** per host including `Crawl-delay`. It is a floor, not proof of permission under a site's ToS.
- `data-pipeline/scraper.py` is **dead legacy code** — a generic HTML scraper whose selectors target long-dead Flipkart class names. `retailer_scraper.py` supersedes it.

Consequences worth knowing before proposing a source:

- `scaffold_manufacturer_evidence.py` emits `specifications` as all-`null` and `observedAt` as `""` on purpose. Never pre-fill specs from `cleaned_data/` — that launders sample planning data into manufacturer evidence. `build_manufacturer_observation` rejects `null`/`""` spec values so an unfilled scaffold cannot land.
- Live prices need an authorized commercial source. Cuelinks cannot supply them (link monetization only, no product data). Keepa's API covers `amazon.in` but starts around €49/mo with no free tier. Full Icecat — which would unlock the 31 `restricted` products — is a paid subscription; only Open Icecat is free.
- `data-pipeline/lake/`, `analytics/`, and `runtime/` are gitignored. The lake therefore has two possible homes, chosen by `open_store(lake_dir)`: the local filesystem by default, or MongoDB when `OBSERVATION_STORE_URI` is set. **Never construct `ObservationStore` directly in new code** — use `open_store()`, or a scheduled run will silently start from an empty lake and re-accept every prior observation as new.

## Observation store backends

`observation_store.py` holds two interchangeable stores behind one contract (`ingest`, `read_observations`, `read_runs`). Both share `partition_batch()` for validation and `jsonl_checksum()` for manifest digests, so accept/duplicate/quarantine counts and checksums are identical either way — `BackendEquivalenceTests` pins that.

- `ObservationStore` — one immutable directory per run under `lake/`. Dedupes by scanning every prior `observations.jsonl`.
- `MongoObservationStore` — collections `observation_records` / `_raw` / `_quarantine` / `_manifests`. Insert-only, with a unique index on `observation_id` enforcing immutability in the database. A concurrent insert losing that race is counted as a duplicate, not an error.

Moving to Atlas is a two-step, both idempotent and dry-run by default:

```bash
npm run lake:migrate         # dry run: what would copy
npm run lake:migrate:apply   # append-only; local files are left in place
```

`.github/workflows/pipeline.yml` runs `pipeline:run` on a daily cron and **fails closed if `OBSERVATION_STORE_URI` is absent**. It needs secrets `OBSERVATION_STORE_URI`, `URI`, `ICECAT_USERNAME`, `ICECAT_PASSWORD`.

Because the admin Data Health console reads five JSON files from the API server's local disk (`services/data-quality.service.js`), a run on any other host must also `publish_reports.py --apply`, which upserts them into `pipeline_reports`. `run_pipeline.py` appends that stage automatically when a URI is configured. **The Node service does not yet read `pipeline_reports`** — until it does, the deployed console still reads local files it does not have.

## Architecture

**Server composition.** `server.js` (dotenv → `assertRuntimeConfig()` → `startDB()` → listen) is separate from `app.js` (the Express app). Tests import `app.js` via supertest without starting a server or DB, so keep `app.js` side-effect free.

**Two route trees.** `routes/routes.js` is the legacy flat surface (`/CPU`, `/login`, `/saves`, …). `routes/api-v1.routes.js` is the current surface under `/api/v1` and mounts the admin sub-routers (`admin-offers`, `admin-content`, `admin-affiliate-links`, `admin-analytics`) plus `privacy`. New endpoints belong in `api-v1`. Both trees delegate business logic to `services/` — routes stay thin.

**Auth and admin.** `middleware/auth.js` verifies a Bearer HS256 JWT and reloads the user, attaching `req.user.isAdmin` from `services/admin-access.service.js`, which derives admin status purely from the `ADMIN_EMAILS` env list (with gmail dot/plus normalization). There is no admin role in the database. Use `requireAdmin` after `authenticate`.

**Evidence separation is the core domain invariant.** The app deliberately never conflates evidence classes:

- Compatibility specs come from the curated catalog and can *never* be modified by an imported feed.
- Prices are classified per-request by `services/catalog-provenance.service.js` as `live` / `stale` / `sample` from `provenance.data_status` and `collected_at` against `CATALOG_FRESHNESS_HOURS` (default 24). Seed data is always `sample`; only the signed admin import path may set live provenance.
- Product content (Open Icecat), retailer offers, benchmarks, and consented build outcomes are appended as separate evidence arrays on `models/componentMetadata.schema.js` (`identity`, `priceHistory`, `productContentEvidence`), never overwritten in place.

`presentCatalogItem()` also strips operator identity (`imported_by`, `importedBy`, `importChecksum`) before anything leaves the API. Any new catalog-returning endpoint must route through it.

**Admin import flow (offers, content, affiliate links).** Upload → server validates and exactly matches rows → returns a preview signed as a 15-minute HS256 JWT → apply accepts only that signed, unexpired preview and records a batch document keyed by feed checksum for idempotency. Matching is by exact manufacturer part number or catalog ID; ambiguous rows are surfaced for manual resolution and the approved mapping is persisted in `retailerProductMapping`. Don't add a path that writes offers/content without a signed preview.

**Compatibility engine.** `services/compatibility.service.js` is pure and synchronous: it takes resolved component documents (never IDs) and returns `{status, checks[], summary, power}`. Routes resolve catalog IDs to documents first. Memory type falls back to a hard-coded chipset→DDR map when the motherboard has no explicit `memory_type`. PSU sizing is `(cpu tdp + gpu tdp + 75W) × 1.2` rounded to the next tier. `data-pipeline/compatibility_engine.py` is a parallel Python implementation used for offline validation — changes to the rules should be mirrored.

**Data pipeline.** `data-pipeline/observation_store.py` is an immutable, append-only landing store (`lake/raw`, `normalized`, `quarantine`, `manifests`) that validates every observation: UTC timestamps, HTTPS source URLs, known kinds/categories, and rejection of sensitive keys. Connectors under `connectors/` (Icecat, Flipkart affiliate, Blender Open Data) only produce observations; promotion into MongoDB always goes through the reviewed/signed Node paths. `run_pipeline.py` orchestrates runs with lock-based non-overlap, atomic JSON writes, and secret redaction of subprocess output. `build_analytics.py` builds the DuckDB/Parquet warehouse under `analytics/`.

**Frontend.** `client/frontEnd/src/services/api.js` is the single axios instance — it injects the `token` from localStorage and clears the session on any 401. `VITE_API_BASE_URL` overrides the base; in dev it defaults to `http://localhost:5000`, in production to the page origin (the Express server serves the built SPA from `client/frontEnd/dist` when `NODE_ENV=production`, with an `/api`-excluding catch-all for client routing). Builder state lives in `Components/builder/` (`buildDraft.js` for draft persistence, `buildUtils.js` for shared logic); session state in `src/auth/SessionContext.jsx`. Tests are colocated as `*.test.jsx` next to the component.

## Constraints to preserve

- Production startup fails closed: `assertRuntimeConfig()` requires Node ≥20.19, and in production an explicit `URI`, a non-placeholder 32+ char `JWT_SECRET`, and HTTPS-only `ALLOWED_ORIGINS`. Don't weaken these checks to make a deploy pass.
- Product analytics are opt-in only, pseudonymized with `ANALYTICS_PSEUDONYM_SECRET`, and deleted on consent revocation (`services/product-analytics.service.js`, `routes/privacy.routes.js`).
- Amazon affiliate links store only an ASIN + exact catalog relationship and a generated tagged URL; no Amazon titles, prices, images, or scraping.
- The UI must keep labeling unverified prices and analytics estimates as planning data. `APP_AUDIT.md` and `CATALOG_DATA_QUALITY.md` record the measured state of these claims — update them when the underlying numbers change.
- CI (`.github/workflows/ci.yml`) runs three independent jobs (backend + catalog against Mongo 8, Python pipeline, frontend) on Node 20.19.0 / Python 3.12.
