const test = require("node:test");
const assert = require("node:assert/strict");
const { classifyPricing, presentCatalogItem, summarizePricing } = require("../services/catalog-provenance.service");

const now = new Date("2026-07-20T12:00:00.000Z");

test("classifies only recent verified records as live", () => {
  assert.equal(classifyPricing({ data_status: "live", collected_at: "2026-07-20T11:00:00.000Z" }, now, 24), "live");
  assert.equal(classifyPricing({ data_status: "live", collected_at: "2026-07-18T11:00:00.000Z" }, now, 24), "stale");
  assert.equal(classifyPricing({ data_status: "fixture", collected_at: "2026-07-20T11:00:00.000Z" }, now, 24), "sample");
});

test("presents safe pricing metadata and summary", () => {
  const live = presentCatalogItem({ name: "CPU", provenance: { data_status: "live", collected_at: "2026-07-20T11:00:00.000Z", source: "flipkart_affiliate", imported_by: "operator@example.com" } }, now, 24);
  const sample = presentCatalogItem({ name: "GPU", provenance: { data_status: "sample" } }, now, 24);
  assert.equal(live.pricing.status, "live");
  assert.equal(live.provenance.imported_by, undefined);
  assert.deepEqual(summarizePricing([live, sample]), { live: 1, stale: 0, sample: 1 });
});
