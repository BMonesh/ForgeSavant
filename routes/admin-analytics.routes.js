const express = require("express");
const { authenticate } = require("../middleware/auth");
const { requireAdmin } = require("../services/admin-access.service");
const { readDataQualitySummary } = require("../services/data-quality.service");

const router = express.Router();

router.use(authenticate, requireAdmin);

router.get("/data-quality", async (_req, res, next) => {
  try {
    return res.json({ data: await readDataQualitySummary() });
  } catch (error) {
    return next(error);
  }
});

module.exports = router;
