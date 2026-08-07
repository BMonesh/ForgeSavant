const test = require("node:test");
const assert = require("node:assert/strict");
const jwt = require("jsonwebtoken");
const request = require("supertest");

process.env.JWT_SECRET = "admin-route-test-secret";
process.env.ADMIN_EMAILS = "admin@example.com";

const app = require("../app");
const User = require("../models/user.model");
const Processor = require("../models/processor.model");
const OfferImportBatch = require("../models/offerImportBatch.model");
const RetailerProductMapping = require("../models/retailerProductMapping.model");

const userId = "507f1f77bcf86cd799439011";
const tokenFor = (email) => jwt.sign({ user: { id: userId, email } }, process.env.JWT_SECRET, { algorithm: "HS256", expiresIn: "15m" });
const mockUser = (t, email) => t.mock.method(User, "findById", () => ({
  select: async () => ({ id: userId, fullname: "Catalog Operator", email }),
}));
const mockMappings = (t, rows = []) => t.mock.method(RetailerProductMapping, "find", () => ({
  lean: async () => rows,
}));

test("offer administration requires authentication", async () => {
  await request(app).get("/api/v1/admin/offers/status").expect(401);
});

test("offer administration rejects authenticated non-admin users", async (t) => {
  mockUser(t, "builder@example.com");
  await request(app)
    .get("/api/v1/admin/offers/status")
    .set("Authorization", `Bearer ${tokenFor("builder@example.com")}`)
    .expect(403);
});

test("admin can preview a valid partner offer without changing catalog data", async (t) => {
  mockUser(t, "admin@example.com");
  mockMappings(t);
  t.mock.method(Processor, "find", () => ({
    lean: async () => [{ _id: userId, name: "AMD Ryzen 5 5600X", price: 14999 }],
  }));
  const response = await request(app)
    .post("/api/v1/admin/offers/preview")
    .set("Authorization", `Bearer ${tokenFor("admin@example.com")}`)
    .send({
      source: "retailer_partner",
      offers: [{
        name: "AMD Ryzen 5 5600X Desktop Processor",
        category: "processors",
        source_item_id: "SKU-5600X",
        price: 12499,
        currency: "INR",
        availability: "in_stock",
        source_url: "https://retailer.example/5600x",
        observed_at: new Date().toISOString(),
      }],
    })
    .expect(200);

  assert.equal(response.body.data.counts.accepted, 1);
  assert.equal(response.body.data.rows[0].match.name, "AMD Ryzen 5 5600X");
  assert.ok(response.body.data.previewToken);
});

test("admin applies only a signed preview and records the batch", async (t) => {
  mockUser(t, "admin@example.com");
  mockMappings(t);
  t.mock.method(Processor, "find", () => ({
    lean: async () => [{ _id: userId, name: "AMD Ryzen 5 5600X", price: 14999 }],
  }));
  let operations;
  t.mock.method(Processor, "bulkWrite", async (nextOperations) => { operations = nextOperations; });
  t.mock.method(OfferImportBatch, "findOne", () => ({ lean: async () => null }));
  t.mock.method(OfferImportBatch, "create", async (batch) => ({ _id: "batch-1", ...batch }));
  let mappingOperations;
  t.mock.method(RetailerProductMapping, "bulkWrite", async (nextOperations) => { mappingOperations = nextOperations; });
  const payload = {
    source: "retailer_partner",
    offers: [{
      name: "AMD Ryzen 5 5600X",
      category: "processors",
      source_item_id: "SKU-5600X",
      price: 12499,
      currency: "INR",
      availability: "in_stock",
      source_url: "https://retailer.example/5600x",
      observed_at: new Date().toISOString(),
    }],
  };
  const authorization = { Authorization: `Bearer ${tokenFor("admin@example.com")}` };
  const preview = await request(app).post("/api/v1/admin/offers/preview").set(authorization).send(payload).expect(200);
  const applied = await request(app).post("/api/v1/admin/offers/apply").set(authorization).send({
    ...payload,
    previewToken: preview.body.data.previewToken,
  }).expect(201);

  assert.equal(operations.length, 1);
  assert.deepEqual(operations[0].updateOne.filter["priceHistory.importChecksum"], { $ne: preview.body.data.checksum });
  assert.equal(operations[0].updateOne.update.$set.price, 12499);
  assert.equal(operations[0].updateOne.update.$set.provenance.data_status, "live");
  assert.equal(operations[0].updateOne.update.$push.priceHistory.price, 12499);
  assert.equal(mappingOperations[0].updateOne.update.$set.componentName, "AMD Ryzen 5 5600X");
  assert.equal(applied.body.data.counts.applied, 1);
});
