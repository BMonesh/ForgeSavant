# ForgeSavant catalog data-quality report

Updated on 2026-07-23 from the live Atlas catalog, reviewed Open Icecat batch, and rebuilt analytical warehouse.

## Outcome

The catalog now contains 58 canonical records across seven hardware categories. A pre-migration audit found one duplicate processor pair with identical specifications. The migration retained `Intel Core i5-12400F`, merged both names and both price observations, updated dependent references, and removed `Intel Core i5 12400F`. A complete JSON backup was written before mutation.

| Check | Before | After | Assessment |
|---|---:|---:|---|
| Catalog records | 59 | 58 | One safe duplicate merged |
| Canonical identity keys | 0 / 59 | 58 / 58 | Complete |
| Records with price history | 0 / 59 | 58 / 58 | Complete baseline |
| Normalized duplicate groups | 1 | 0 | Resolved |
| Saved builds with orphan references | 0 | 0 | Passed |
| Retailer mappings with orphan references | 0 | 0 | Passed |
| Manufacturer part numbers | 0 / 59 | 58 / 58 | All exact retail products manufacturer-verified |
| Records with source and observation timestamp | 0 / 59 | 0 / 58 | Missing upstream observation times |
| Live retailer prices | 0 / 59 | 0 / 58 | No authorized feed imported yet |
| Products with reviewed content evidence | 0 / 59 | 14 / 58 | Exact-MPN Open Icecat promotion applied |
| Current evidence with complete identity | — | 14 / 14 | Passed |
| Current evidence with image | — | 14 / 14 | Passed |
| Current evidence with GTIN | — | 13 / 14 | 92.86% coverage |
| Quarantined observations | — | 0 / 42 received | Passed |

## Category coverage

| Category | Records |
|---|---:|
| Processors | 13 |
| Graphics cards | 11 |
| Motherboards | 12 |
| Memory | 10 |
| Storage | 4 |
| Power supplies | 6 |
| Cabinets | 2 |

## Findings and remediation

1. **High — catalog prices are not yet market observations.** All 58 prices remain planning/sample data. The application labels them accordingly and does not claim they are live. Remediation: import an authorized retailer or partner feed through `/admin/offers`; every approved row records source, URL, SKU, availability, observed time, and price history.
2. **Medium — the development dataset lacks retailer observation timestamps and offer URLs.** Manufacturer identity evidence is complete, but it is intentionally separate from price provenance. Remediation: replace sample pricing only with authorized, verifiable observations.
3. **Low — retailer mapping coverage is currently zero.** This is expected before the first partner feed. Accepted automatic or manual matches are persisted, so repeat imports resolve the same source SKU deterministically.

4. **Medium — product-content coverage is partial.** Open Icecat exposes reviewed content for 14 of 58 catalog products and does not currently cover processors, power supplies, or cabinets. The application presents this as optional evidence and does not infer missing specifications.
5. **High — predictive modeling is not yet evidence-ready.** The warehouse has one ingestion date, no supervised outcomes, no independent benchmark labels, and no retailer price time series. Descriptive monitoring is valid; recommendation and forecasting models remain blocked by explicit gates.

## Controls now in place

- Stable canonical identity, aliases, lifecycle state, and price-history fields on all component types.
- Verified manufacturer part numbers are stored with their manufacturer evidence URL and verification time; partner-feed matching prioritizes exact MPNs over title similarity.
- Persistent `(source, source_item_id)` retailer mapping with automatic/manual provenance.
- Reviewed Amazon.in ASIN destinations are stored as `affiliate_link`
  relationships, disclosed to users, and excluded from retailer-price coverage,
  offer freshness, and price-history metrics.
- Signed 15-minute feed previews; changed resolutions invalidate the previous preview.
- Only accepted matches can update price, availability, image, and provenance; compatibility specifications remain curated.
- Idempotent import batches and per-record checksum guards prevent the same reviewed feed from being applied twice, including concurrent retries.
- Public component evidence pages expose identity, structured specifications, current price status, history, and retailer mappings.
- Immutable observation history and a latest-per-source-product analytical view prevent corrected identities from inflating current coverage.
- Product-content imports are previewed with exact manufacturer part numbers, signed for 15 minutes, applied idempotently, and kept separate from compatibility and pricing fields.

Re-run `npm run catalog:quality` after every catalog migration or partner-feed import. Any duplicate group, orphaned mapping, or orphaned saved-build reference should block deployment.
