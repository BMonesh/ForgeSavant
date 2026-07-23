const test = require("node:test");
const assert = require("node:assert/strict");
const request = require("supertest");
const app = require("../app");
const Processor = require("../models/processor.model");
const RetailerProductMapping = require("../models/retailerProductMapping.model");

const componentId = "507f1f77bcf86cd799439011";

test("catalog detail exposes identity and evidence without operator identity", async (t) => {
  t.mock.method(Processor, "findById", () => ({
    lean: async () => ({
      _id: componentId,
      name: "AMD Ryzen 5 5600X",
      manufacturer: "AMD",
      price: 12499,
      specifications: { socket: "AM4" },
      identity: { canonicalKey: "processors:amd-ryzen-5-5600x", aliases: [] },
      provenance: { source: "partner", imported_by: "operator@example.com", data_status: "live", collected_at: new Date().toISOString() },
      priceHistory: [{ price: 12499, source: "partner", observedAt: new Date() }],
      productContentEvidence: [{ observationId: "obs-1", source: "open_icecat", importedBy: "operator@example.com", importChecksum: "private-checksum" }],
    }),
  }));
  t.mock.method(RetailerProductMapping, "find", () => ({
    select: () => ({ lean: async () => [{ source: "partner", sourceItemId: "SKU-1", matchMethod: "manual" }] }),
  }));

  const response = await request(app).get(`/api/v1/catalog/processors/${componentId}`).expect(200);
  assert.equal(response.body.data.identity.canonicalKey, "processors:amd-ryzen-5-5600x");
  assert.equal(response.body.data.priceHistory.length, 1);
  assert.equal(response.body.data.retailerMappings.length, 1);
  assert.equal(response.body.data.provenance.imported_by, undefined);
  assert.equal(response.body.data.productContentEvidence[0].importedBy, undefined);
  assert.equal(response.body.data.productContentEvidence[0].importChecksum, undefined);
});

test("catalog detail validates category and component id", async () => {
  await request(app).get(`/api/v1/catalog/unknown/${componentId}`).expect(404);
  await request(app).get("/api/v1/catalog/processors/not-an-id").expect(400);
});
