const test = require("node:test");
const assert = require("node:assert/strict");
const AnalyticsEvent = require("../models/analyticsEvent.model");
const {
  componentTotal,
  pseudonym,
  recordBuildOutcome,
} = require("../services/product-analytics.service");

process.env.JWT_SECRET = "product-analytics-test-secret";

test("creates stable namespace-separated pseudonyms", () => {
  assert.equal(pseudonym("user", "123"), pseudonym("user", "123"));
  assert.notEqual(pseudonym("user", "123"), pseudonym("build", "123"));
  assert.doesNotMatch(pseudonym("user", "private@example.com"), /private|example/);
});

test("records only explicitly consented build outcomes", async (t) => {
  const calls = [];
  t.mock.method(AnalyticsEvent, "create", async (value) => { calls.push(value); return value; });
  const common = {
    eventType: "build_saved",
    savedBuild: {
      id: "507f1f77bcf86cd799439011",
      componentIds: {
        processor: "507f1f77bcf86cd799439012",
        motherboard: "507f1f77bcf86cd799439013",
        gpu: "507f1f77bcf86cd799439014",
        primaryStorage: "507f1f77bcf86cd799439015",
        ram: "507f1f77bcf86cd799439016",
        smps: "507f1f77bcf86cd799439017",
        cabinet: "507f1f77bcf86cd799439018",
      },
    },
    components: { processor: { price: 10000 }, gpu: { price: 20000 } },
    compatibility: { status: "compatible", engine: { version: "compat-1" } },
    analytics: { model: { version: "planning-1" } },
  };

  await recordBuildOutcome({ ...common, user: { id: "user-1", analyticsConsent: false } });
  assert.equal(calls.length, 0);

  await recordBuildOutcome({ ...common, user: { id: "user-1", analyticsConsent: true } });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].buildTotal, 30000);
  assert.equal(calls[0].currency, "INR");
  assert.equal(calls[0].subjectHash.length, 64);
  assert.equal(componentTotal(common.components), 30000);
});
