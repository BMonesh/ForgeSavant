const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const jwt = require("jsonwebtoken");
const request = require("supertest");

process.env.JWT_SECRET = "analytics-route-test-secret";
process.env.ADMIN_EMAILS = "admin@example.com";

const app = require("../app");
const User = require("../models/user.model");

const userId = "507f1f77bcf86cd799439011";
const tokenFor = (email) => jwt.sign({ user: { id: userId, email } }, process.env.JWT_SECRET, { algorithm: "HS256", expiresIn: "15m" });
const mockUser = (t, email) => t.mock.method(User, "findById", () => ({
  select: async () => ({ id: userId, fullname: "Data Operator", email }),
}));

const fixture = {
  schemaVersion: "1.0",
  generatedAt: new Date().toISOString(),
  grain: "one observation per source product content version",
  catalog: { verifiedProducts: 58, observedProducts: 14, openIcecatAvailable: 14, openIcecatRestricted: 31, openIcecatUnavailable: 13 },
  pipeline: { runs: 1, received: 14, accepted: 14, duplicates: 0, quarantined: 0, latestRunAt: new Date().toISOString() },
  retail: { priceObservations: 8, productsWithPriceHistory: 4, currentOffers: 4, productsWithCurrentOffers: 4, retailers: 1, validPriceRate: 1, latestObservedAt: new Date().toISOString() },
  coverageQueue: { verified: 58, covered: 14, manufacturerReady: 44, sourceMissing: 0 },
  quality: { identityCompletenessRate: 1, gtinCoverageRate: 13 / 14, imageCoverageRate: 1, quarantineRate: 0 },
  categories: { gpus: { observations: 6, distinctProducts: 6, completeIdentity: 6, withGtin: 6, withImage: 6, verifiedCatalogProducts: 11, sourceCoverage: 6 } },
  caveats: ["Product content is not retailer pricing."],
};

test("data-quality dashboard requires authentication", async () => {
  await request(app).get("/api/v1/admin/analytics/data-quality").expect(401);
});

test("data-quality dashboard rejects non-admin users", async (t) => {
  mockUser(t, "builder@example.com");
  await request(app)
    .get("/api/v1/admin/analytics/data-quality")
    .set("Authorization", `Bearer ${tokenFor("builder@example.com")}`)
    .expect(403);
});

test("admin receives reconciled metrics without filesystem paths", async (t) => {
  mockUser(t, "admin@example.com");
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "forgesavant-quality-"));
  const summaryPath = path.join(directory, "summary.json");
  const retailSnapshotPath = path.join(directory, "retail-snapshot.json");
  fs.writeFileSync(summaryPath, JSON.stringify({ ...fixture, sourcePath: "C:/private/path" }));
  fs.writeFileSync(retailSnapshotPath, JSON.stringify({
    schemaVersion: "1.0", snapshotAt: new Date().toISOString(), scannedComponents: 58,
    scannedPriceHistoryEntries: 59, eligibleOffers: 0, accepted: 0, duplicates: 0,
    quarantined: 0, skipped: Array.from({ length: 59 }, () => ({ reason: "sourceItemId is required" })),
  }));
  process.env.DATA_QUALITY_SUMMARY_PATH = summaryPath;
  process.env.RETAIL_SNAPSHOT_PATH = retailSnapshotPath;
  t.after(() => { delete process.env.DATA_QUALITY_SUMMARY_PATH; delete process.env.RETAIL_SNAPSHOT_PATH; fs.rmSync(directory, { recursive: true, force: true }); });

  const response = await request(app)
    .get("/api/v1/admin/analytics/data-quality")
    .set("Authorization", `Bearer ${tokenFor("admin@example.com")}`)
    .expect(200);

  assert.equal(response.body.data.status, "healthy");
  assert.equal(response.body.data.catalog.coverageRate, 14 / 58);
  assert.equal(response.body.data.pipeline.validationPassRate, 1);
  assert.equal(response.body.data.retail.priceObservations, 8);
  assert.equal(response.body.data.coverageQueue.manufacturerReady, 44);
  assert.equal(response.body.data.retailSnapshot.scannedComponents, 58);
  assert.equal(response.body.data.retailSnapshot.skippedEntries, 59);
  assert.equal(response.body.data.sourcePath, undefined);
  assert.match(response.body.data.definitions.catalogCoverage, /verified catalog products/i);
});
