const test = require("node:test");
const assert = require("node:assert/strict");
const {
  GAMING_MODEL_VERSION,
  chipVendor,
  estimateGamingTiers,
  projectedFrameRate,
} = require("../services/gaming-estimate.service");

const gpu = (name) => ({ name });
const record = (metricValue) => ({ metricValue, benchmarkName: "Blender Open Data 5.1.1" });
const bands = (result) => Object.fromEntries(result.tiers.map((tier) => [tier.resolution, tier.band]));

test("an RTX 4060-class score lands at the 1080p anchor", () => {
  assert.equal(Math.round(projectedFrameRate(3000, 1920 * 1080)), 60);
});

test("higher resolutions cost less than their pixel count alone would suggest", () => {
  const full = projectedFrameRate(3000, 1920 * 1080);
  const fourK = projectedFrameRate(3000, 3840 * 2160);
  assert.ok(fourK < full / 2, "4K is much slower than 1080p");
  assert.ok(fourK > full / 4, "but not four times slower");
});

test("bands an RTX 40 series card at each resolution from its benchmark score", () => {
  const result = estimateGamingTiers(gpu("ASUS Dual GeForce RTX 4060 OC Edition 8GB"), record(3063));
  assert.equal(result.status, "estimate");
  assert.equal(result.confidence, "low");
  assert.equal(result.model.version, GAMING_MODEL_VERSION);
  assert.deepEqual(bands(result), { "1080p": "60-90", "1440p": "30-60", "4k": "under-30" });
});

test("a faster card in the same generation lands in higher bands", () => {
  const result = estimateGamingTiers(gpu("ASUS TUF Gaming GeForce RTX 4090 OC Edition 24GB"), record(11663));
  assert.deepEqual(bands(result), { "1080p": "144-plus", "1440p": "144-plus", "4k": "60-90" });
});

test("never returns a raw frame-rate number", () => {
  const result = estimateGamingTiers(gpu("GeForce RTX 4070"), record(5251));
  for (const tier of result.tiers) {
    assert.deepEqual(Object.keys(tier).sort(), ["band", "bandLabel", "label", "resolution"]);
  }
});

test("labels the result as an estimate that no game was run for", () => {
  const result = estimateGamingTiers(gpu("GeForce RTX 4070"), record(5251));
  assert.match(result.assumptions.join(" "), /not measured/i);
});

test("refuses Radeon cards, whose Blender scores understate gaming performance", () => {
  const result = estimateGamingTiers(gpu("SAPPHIRE PULSE Radeon RX 7800 XT 16GB"), record(2872));
  assert.equal(result.status, "unavailable");
  assert.deepEqual(result.tiers, []);
  assert.match(result.reason, /RX 7800 XT below an RTX 4060/);
});

test("refuses earlier GeForce generations rather than under-rating them", () => {
  const result = estimateGamingTiers(gpu("ZOTAC GAMING GeForce RTX 3060 Twin Edge OC 12GB"), record(1989));
  assert.equal(result.status, "unavailable");
  assert.match(result.reason, /RTX 40 series/);
});

test("refuses a calibrated card that has no matching benchmark", () => {
  const result = estimateGamingTiers(gpu("GeForce RTX 4070"), null);
  assert.equal(result.status, "unavailable");
  assert.match(result.reason, /No public benchmark/);
});

test("asks for a graphics card when none is selected", () => {
  assert.equal(estimateGamingTiers(null, record(3000)).status, "unavailable");
});

test("identifies the chip vendor from the product name, not the board partner", () => {
  assert.equal(chipVendor("SAPPHIRE NITRO+ Radeon RX 7900 XTX"), "amd");
  assert.equal(chipVendor("ZOTAC GAMING GeForce RTX 3060"), "nvidia");
  assert.equal(chipVendor("Intel Arc A770"), "intel");
});
