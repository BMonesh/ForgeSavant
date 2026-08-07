const test = require("node:test");
const assert = require("node:assert/strict");
const {
  buildAmazonIndiaUrl,
  normalizeAmazonLink,
  reviewAmazonLinks,
} = require("../services/affiliate-link.service");

const componentId = "507f1f77bcf86cd799439011";
const associateTag = "forgesavantpc-21";
const catalogRows = [{
  _id: componentId,
  name: "AMD Ryzen 5 5600X",
  identity: { manufacturerPartNumber: "100-100000065BOX" },
}];
const catalogModels = {
  processors: {
    find: () => ({
      select: () => ({ lean: async () => catalogRows }),
    }),
  },
};

test("constructs a fixed Amazon.in destination without accepting arbitrary URLs", () => {
  assert.equal(
    buildAmazonIndiaUrl("B09V2W32QX", associateTag),
    "https://www.amazon.in/dp/B09V2W32QX?tag=forgesavantpc-21"
  );
  assert.throws(() => buildAmazonIndiaUrl("not-an-asin", associateTag), /ASIN/);
});

test("normalizes Amazon links and requires an exact catalog identifier", () => {
  const valid = normalizeAmazonLink({
    asin: "b09v2w32qx",
    category: "cpu",
    manufacturer_part_number: "100-100000065box",
  }, 0);
  assert.deepEqual(valid.errors, []);
  assert.equal(valid.link.asin, "B09V2W32QX");
  assert.equal(valid.link.category, "processors");

  const invalid = normalizeAmazonLink({ asin: "B09V2W32QX", category: "cpu" }, 0);
  assert.ok(invalid.errors.includes("component_id or manufacturer_part_number is required"));
});

test("reviews exact-MPN links without creating price observations", async () => {
  const review = await reviewAmazonLinks({
    associateTag,
    catalogModels,
    links: [{
      asin: "B09V2W32QX",
      category: "processors",
      manufacturer_part_number: "100-100000065BOX",
    }],
  });

  assert.deepEqual(review.counts, {
    received: 1,
    accepted: 1,
    ambiguous: 0,
    unmatched: 0,
    rejected: 0,
  });
  assert.equal(review.rows[0].match.id, componentId);
  assert.equal(review.rows[0].link.price, undefined);
  assert.equal(review.rows[0].link.source, "amazon.in");
});

test("rejects duplicate ASINs and unmatched identifiers", async () => {
  const review = await reviewAmazonLinks({
    associateTag,
    catalogModels,
    links: [
      { asin: "B09V2W32QX", category: "processors", manufacturer_part_number: "MISSING" },
      { asin: "B09V2W32QX", category: "processors", manufacturer_part_number: "100-100000065BOX" },
    ],
  });
  assert.equal(review.counts.rejected, 2);
});
