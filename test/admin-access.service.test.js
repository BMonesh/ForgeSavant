const assert = require("node:assert/strict");
const test = require("node:test");
const { isAdminEmail, normalizeAdminEmail } = require("../services/admin-access.service");

test("normalizes Gmail dots, subaddresses, and googlemail aliases", () => {
  assert.equal(normalizeAdminEmail("2005.monesh+qa@googlemail.com"), "2005monesh@gmail.com");
});

test("matches a normalized login email to a dotted Gmail admin entry", () => {
  const previous = process.env.ADMIN_EMAILS;
  process.env.ADMIN_EMAILS = "2005.monesh@gmail.com";
  try {
    assert.equal(isAdminEmail("2005monesh@gmail.com"), true);
  } finally {
    if (previous === undefined) delete process.env.ADMIN_EMAILS;
    else process.env.ADMIN_EMAILS = previous;
  }
});

test("does not rewrite dots or subaddresses for non-Gmail domains", () => {
  assert.equal(normalizeAdminEmail("Admin.Test+qa@example.com"), "admin.test+qa@example.com");
});
