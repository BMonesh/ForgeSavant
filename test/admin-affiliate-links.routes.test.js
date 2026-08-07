const test = require("node:test");
const assert = require("node:assert/strict");
const jwt = require("jsonwebtoken");
const request = require("supertest");

process.env.JWT_SECRET = "affiliate-route-test-secret";
process.env.ADMIN_EMAILS = "admin@example.com";
process.env.AMAZON_ASSOCIATE_TAG = "forgesavantpc-21";

const app = require("../app");
const User = require("../models/user.model");
const Processor = require("../models/processor.model");
const AffiliateLinkImportBatch = require("../models/affiliateLinkImportBatch.model");
const RetailerProductMapping = require("../models/retailerProductMapping.model");

const userId = "507f1f77bcf86cd799439011";
const token = jwt.sign(
  { user: { id: userId, email: "admin@example.com" } },
  process.env.JWT_SECRET,
  { algorithm: "HS256", expiresIn: "15m" }
);
const payload = {
  links: [{
    asin: "B09V2W32QX",
    category: "processors",
    manufacturer_part_number: "100-100000065BOX",
  }],
};

const mockUser = (t) => t.mock.method(User, "findById", () => ({
  select: async () => ({ id: userId, fullname: "Catalog Operator", email: "admin@example.com" }),
}));
const mockCatalog = (t) => t.mock.method(Processor, "find", () => ({
  select: () => ({
    lean: async () => [{
      _id: userId,
      name: "AMD Ryzen 5 5600X",
      identity: { manufacturerPartNumber: "100-100000065BOX" },
    }],
  }),
}));

test("affiliate-link administration requires authentication", async () => {
  await request(app).get("/api/v1/admin/affiliate-links/status").expect(401);
});

test("admin previews and applies an exact Amazon.in mapping without a price update", async (t) => {
  mockUser(t);
  mockCatalog(t);
  t.mock.method(AffiliateLinkImportBatch, "findOne", () => ({ lean: async () => null }));
  t.mock.method(AffiliateLinkImportBatch, "create", async (batch) => ({ _id: "batch-1", ...batch }));
  let mappingOperations;
  t.mock.method(RetailerProductMapping, "bulkWrite", async (operations) => { mappingOperations = operations; });

  const authorization = { Authorization: `Bearer ${token}` };
  const preview = await request(app)
    .post("/api/v1/admin/affiliate-links/preview")
    .set(authorization)
    .send(payload)
    .expect(200);
  assert.equal(preview.body.data.counts.accepted, 1);

  const applied = await request(app)
    .post("/api/v1/admin/affiliate-links/apply")
    .set(authorization)
    .send({ ...payload, previewToken: preview.body.data.previewToken })
    .expect(201);

  const mapping = mappingOperations[0].updateOne.update.$set;
  assert.equal(mapping.relationshipType, "affiliate_link");
  assert.equal(mapping.sourceUrl, "https://www.amazon.in/dp/B09V2W32QX?tag=forgesavantpc-21");
  assert.equal(mapping.price, undefined);
  assert.equal(applied.body.data.counts.applied, 1);
});
