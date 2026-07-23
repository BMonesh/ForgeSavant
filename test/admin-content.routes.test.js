const test = require("node:test");
const assert = require("node:assert/strict");
const jwt = require("jsonwebtoken");
const request = require("supertest");

process.env.JWT_SECRET = "content-route-test-secret";
process.env.ADMIN_EMAILS = "admin@example.com";

const app = require("../app");
const User = require("../models/user.model");
const GraphicsCard = require("../models/graphicsCard.model");
const ContentImportBatch = require("../models/contentImportBatch.model");

const componentId = "507f1f77bcf86cd799439011";
const tokenFor = (email) => jwt.sign({ user: { id: componentId, email } }, process.env.JWT_SECRET, { algorithm: "HS256", expiresIn: "15m" });
const mockUser = (t, email) => t.mock.method(User, "findById", () => ({
  select: async () => ({ id: componentId, fullname: "Content Operator", email }),
}));
const observation = {
  schema_version: "1.0",
  observation_id: "a".repeat(64),
  observation_kind: "product_content",
  source: "open_icecat",
  source_tier: "open",
  source_product_id: "icecat-1",
  catalog_category: "gpus",
  catalog_name: "ASUS Example GPU",
  manufacturer: "ASUS",
  manufacturer_part_number: "GPU-EXAMPLE-1",
  name: "ASUS Example GPU source title",
  gtins: ["1234567890123"],
  specifications: { cuda_cores: "3072", memory: "8 GB" },
  image_url: "https://images.example/gpu.jpg",
  manufacturer_url: "https://manufacturer.example/gpu",
  source_record_url: "https://data.example/gpu.xml",
  observed_at: "2026-07-20T00:00:00.000Z",
  ingested_at: "2026-07-20T00:01:00.000Z",
  raw_sha256: "b".repeat(64),
};

const mockCatalog = (t) => t.mock.method(GraphicsCard, "find", () => ({
  select: () => ({ lean: async () => [{
    _id: componentId,
    name: "ASUS Example GPU",
    manufacturer: "ASUS",
    identity: { manufacturerPartNumber: "GPU-EXAMPLE-1" },
  }] }),
}));

test("content administration requires authentication", async () => {
  await request(app).get("/api/v1/admin/content/history").expect(401);
});

test("admin previews content using exact manufacturer part number matching", async (t) => {
  mockUser(t, "admin@example.com");
  mockCatalog(t);
  const response = await request(app)
    .post("/api/v1/admin/content/preview")
    .set("Authorization", `Bearer ${tokenFor("admin@example.com")}`)
    .send({ observations: [observation] })
    .expect(200);

  assert.equal(response.body.data.counts.accepted, 1);
  assert.equal(response.body.data.rows[0].match.id, componentId);
  assert.ok(response.body.data.previewToken);
});

test("admin applies a signed content preview without changing curated specifications", async (t) => {
  mockUser(t, "admin@example.com");
  mockCatalog(t);
  let operations;
  t.mock.method(GraphicsCard, "bulkWrite", async (next) => { operations = next; return { modifiedCount: 1 }; });
  t.mock.method(ContentImportBatch, "findOne", () => ({ lean: async () => null }));
  t.mock.method(ContentImportBatch, "create", async (batch) => ({ _id: "batch-1", ...batch }));
  const authorization = { Authorization: `Bearer ${tokenFor("admin@example.com")}` };
  const payload = { observations: [observation] };
  const preview = await request(app).post("/api/v1/admin/content/preview").set(authorization).send(payload).expect(200);
  const applied = await request(app).post("/api/v1/admin/content/apply").set(authorization).send({
    ...payload,
    previewToken: preview.body.data.previewToken,
  }).expect(201);

  const update = operations[0].updateOne.update;
  assert.equal(update.$set, undefined);
  assert.equal(update.$push.productContentEvidence.observationId, observation.observation_id);
  assert.equal(update.$push.productContentEvidence.importedBy, "admin@example.com");
  assert.equal(applied.body.data.counts.applied, 1);
});

test("content preview rejects unsafe nested specification values", async (t) => {
  mockUser(t, "admin@example.com");
  const response = await request(app)
    .post("/api/v1/admin/content/preview")
    .set("Authorization", `Bearer ${tokenFor("admin@example.com")}`)
    .send({ observations: [{ ...observation, specifications: { unsafe: { nested: true } } }] })
    .expect(200);
  assert.equal(response.body.data.counts.rejected, 1);
  assert.match(response.body.data.rows[0].errors.join(" "), /scalar value/);
});
