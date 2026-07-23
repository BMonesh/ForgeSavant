const test = require("node:test");
const assert = require("node:assert/strict");
const {
  normalizeOffer,
  scoreTitleMatch,
  matchOffer,
  reviewOffers,
} = require("../services/offer-import.service");

const now = new Date("2026-07-21T10:00:00.000Z");
const validOffer = {
  name: "AMD Ryzen 5 5600X Desktop Processor",
  category: "cpu",
  source_item_id: "partner-5600x",
  price: 12499,
  currency: "INR",
  availability: "in stock",
  source_url: "https://retailer.example/products/5600x",
  observed_at: "2026-07-21T09:30:00.000Z",
};

test("normalizes the partner feed contract and rejects unsafe URLs", () => {
  const normalized = normalizeOffer(validOffer, 0, now);
  assert.deepEqual(normalized.errors, []);
  assert.equal(normalized.offer.category, "processors");
  assert.equal(normalized.offer.availability, "in_stock");

  const invalid = normalizeOffer({ ...validOffer, source_url: "http://retailer.example/item" }, 0, now);
  assert.ok(invalid.errors.includes("source_url must be an HTTPS URL"));
});

test("matches exact models while rejecting nearby components", () => {
  assert.equal(scoreTitleMatch("AMD Ryzen 5 5600X Processor", "AMD Ryzen 5 5600X"), 1);
  const catalog = [
    { _id: "1", name: "AMD Ryzen 5 5600X", price: 14999 },
    { _id: "2", name: "AMD Ryzen 7 5800X", price: 22999 },
  ];
  const match = matchOffer(validOffer, catalog);
  assert.equal(match.status, "accepted");
  assert.equal(match.match.name, "AMD Ryzen 5 5600X");
  assert.equal(matchOffer({ ...validOffer, name: "AMD Ryzen 9 9950X" }, catalog).status, "unmatched");
});

test("manufacturer part numbers override noisy retailer titles", () => {
  const catalog = [
    { _id: "1", name: "AMD Ryzen 5 5600X", price: 14999, identity: { manufacturerPartNumber: "100-100000065BOX" } },
    { _id: "2", name: "AMD Ryzen 7 5800X", price: 22999, identity: { manufacturerPartNumber: "100-100000063WOF" } },
  ];
  const normalized = normalizeOffer({ ...validOffer, name: "AMD CPU special offer", manufacturer_part_number: "100-100000063wof" }, 0, now);
  const match = matchOffer(normalized.offer, catalog);
  assert.equal(match.status, "accepted");
  assert.equal(match.match.name, "AMD Ryzen 7 5800X");
  assert.equal(match.matchSignal, "manufacturer_part_number");
});

test("reviews valid, unmatched, and rejected rows without applying changes", async () => {
  const processorRows = [{ _id: "507f1f77bcf86cd799439011", name: "AMD Ryzen 5 5600X", price: 14999 }];
  const catalogModels = {
    processors: { find: () => ({ lean: async () => processorRows }) },
  };
  const review = await reviewOffers({
    source: "partner_feed",
    offers: [
      validOffer,
      { ...validOffer, name: "Intel Core i9 99999K", source_item_id: "missing" },
      { ...validOffer, price: -1, source_item_id: "invalid" },
    ],
    catalogModels,
    now,
  });
  assert.deepEqual(review.counts, { received: 3, accepted: 1, ambiguous: 0, unmatched: 1, rejected: 1 });
  assert.equal(review.rows[0].match.name, "AMD Ryzen 5 5600X");
  assert.match(review.checksum, /^[a-f0-9]{64}$/);
});

test("manual resolutions are signed and saved mappings take priority", async () => {
  const processorRows = [
    { _id: "507f1f77bcf86cd799439011", name: "AMD Ryzen 5 5600X", price: 14999 },
    { _id: "507f1f77bcf86cd799439012", name: "AMD Ryzen 7 5700X", price: 18999 },
  ];
  const catalogModels = { processors: { find: () => ({ lean: async () => processorRows }) } };
  const unresolved = { ...validOffer, name: "Ryzen desktop chip", source_item_id: "manual-sku" };
  const manual = await reviewOffers({
    source: "partner_feed",
    offers: [unresolved],
    catalogModels,
    resolutions: [{ index: 0, componentId: processorRows[1]._id }],
    now,
  });
  assert.equal(manual.rows[0].matchMethod, "manual");
  assert.equal(manual.rows[0].match.id, processorRows[1]._id);

  const mapped = await reviewOffers({
    source: "partner_feed",
    offers: [unresolved],
    catalogModels,
    mappings: [{ sourceItemId: "manual-sku", componentId: processorRows[0]._id, matchMethod: "manual", confidence: 1, active: true }],
    now,
  });
  assert.equal(mapped.rows[0].matchedBy, "saved_mapping");
  assert.equal(mapped.rows[0].match.id, processorRows[0]._id);
  assert.notEqual(manual.checksum, mapped.checksum);
});
