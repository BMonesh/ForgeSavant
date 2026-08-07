const test = require("node:test");
const assert = require("node:assert/strict");
const jwt = require("jsonwebtoken");
const request = require("supertest");

process.env.JWT_SECRET = "privacy-route-test-secret";

const app = require("../app");
const User = require("../models/user.model");
const AnalyticsEvent = require("../models/analyticsEvent.model");

const userId = "507f1f77bcf86cd799439011";
const token = jwt.sign({ user: { id: userId, email: "builder@example.com" } }, process.env.JWT_SECRET, {
  algorithm: "HS256",
  expiresIn: "15m",
});

const mockUser = (t, consent = false) => t.mock.method(User, "findById", () => ({
  select: async () => ({
    id: userId,
    fullname: "Builder",
    email: "builder@example.com",
    analyticsConsent: consent,
    analyticsConsentedAt: consent ? new Date("2026-07-23T00:00:00Z") : null,
  }),
}));

test("analytics privacy settings require authentication", async () => {
  await request(app).get("/api/v1/privacy/analytics").expect(401);
});

test("returns the current opt-in state", async (t) => {
  mockUser(t, true);
  const response = await request(app)
    .get("/api/v1/privacy/analytics")
    .set("Authorization", `Bearer ${token}`)
    .expect(200);
  assert.equal(response.body.data.enabled, true);
  assert.equal(response.body.data.policyVersion, "1.0");
});

test("opting out deletes pseudonymous outcome events", async (t) => {
  mockUser(t, true);
  t.mock.method(User, "updateOne", async () => ({ modifiedCount: 1 }));
  let deletionFilter;
  t.mock.method(AnalyticsEvent, "deleteMany", async (filter) => {
    deletionFilter = filter;
    return { deletedCount: 3 };
  });
  const response = await request(app)
    .patch("/api/v1/privacy/analytics")
    .set("Authorization", `Bearer ${token}`)
    .send({ enabled: false })
    .expect(200);
  assert.equal(response.body.data.enabled, false);
  assert.equal(response.body.data.deletedEvents, 3);
  assert.equal(deletionFilter.subjectHash.length, 64);
});

test("rejects ambiguous consent values", async (t) => {
  mockUser(t, false);
  await request(app)
    .patch("/api/v1/privacy/analytics")
    .set("Authorization", `Bearer ${token}`)
    .send({ enabled: "yes" })
    .expect(400);
});
