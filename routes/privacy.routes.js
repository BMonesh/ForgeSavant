const express = require("express");
const { authenticate } = require("../middleware/auth");
const User = require("../models/user.model");
const { deleteSubjectEvents } = require("../services/product-analytics.service");

const router = express.Router();

router.use(authenticate);

router.get("/analytics", (req, res) => res.json({
  data: {
    enabled: Boolean(req.user.analyticsConsent),
    consentedAt: req.user.analyticsConsentedAt || null,
    policyVersion: "1.0",
  },
}));

router.patch("/analytics", async (req, res, next) => {
  try {
    if (typeof req.body?.enabled !== "boolean") {
      return res.status(400).json({ error: "enabled must be a boolean" });
    }
    const update = req.body.enabled
      ? { analyticsConsent: true, analyticsConsentedAt: new Date(), analyticsConsentVersion: "1.0" }
      : { analyticsConsent: false, analyticsConsentedAt: null, analyticsConsentVersion: "1.0" };
    await User.updateOne({ _id: req.user.id }, { $set: update });
    let deletedEvents = 0;
    if (!req.body.enabled) {
      const result = await deleteSubjectEvents(req.user.id);
      deletedEvents = result.deletedCount || 0;
    }
    return res.json({
      data: {
        enabled: req.body.enabled,
        consentedAt: update.analyticsConsentedAt,
        policyVersion: "1.0",
        deletedEvents,
      },
    });
  } catch (error) {
    return next(error);
  }
});

module.exports = router;
