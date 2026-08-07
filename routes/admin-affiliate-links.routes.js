const express = require("express");
const jwt = require("jsonwebtoken");
const { authenticate } = require("../middleware/auth");
const { requireAdmin } = require("../services/admin-access.service");
const { reviewAmazonLinks } = require("../services/affiliate-link.service");
const AffiliateLinkImportBatch = require("../models/affiliateLinkImportBatch.model");
const RetailerProductMapping = require("../models/retailerProductMapping.model");
const Processor = require("../models/processor.model");
const GraphicsCard = require("../models/graphicsCard.model");
const Motherboard = require("../models/motherboard.model");
const RAM = require("../models/ram.model");
const Storage = require("../models/storage.model");
const SMPS = require("../models/smps.model");
const Cabinet = require("../models/cabinet.model");

const router = express.Router();
const catalogModels = {
  processors: Processor,
  gpus: GraphicsCard,
  motherboards: Motherboard,
  ram: RAM,
  storage: Storage,
  powerSupplies: SMPS,
  cabinets: Cabinet,
};

router.use(authenticate, requireAdmin);

const previewToken = (checksum, email) => jwt.sign(
  { kind: "affiliate-link-preview", checksum, email },
  process.env.JWT_SECRET,
  { algorithm: "HS256", expiresIn: "15m" }
);

const validPreviewToken = (token, checksum, email) => {
  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET, { algorithms: ["HS256"] });
    return decoded.kind === "affiliate-link-preview" && decoded.checksum === checksum && decoded.email === email;
  } catch {
    return false;
  }
};

const reviewRequest = (body) => reviewAmazonLinks({
  links: body?.links,
  associateTag: process.env.AMAZON_ASSOCIATE_TAG,
  catalogModels,
});

router.get("/status", (req, res) => {
  res.json({
    data: {
      configured: Boolean(process.env.AMAZON_ASSOCIATE_TAG),
      marketplace: "amazon.in",
      maxRecords: 500,
    },
  });
});

router.get("/history", async (req, res, next) => {
  try {
    const batches = await AffiliateLinkImportBatch.find().sort({ createdAt: -1 }).limit(20).lean();
    return res.json({ data: batches });
  } catch (error) {
    return next(error);
  }
});

router.post("/preview", async (req, res, next) => {
  try {
    const review = await reviewRequest(req.body);
    return res.json({
      data: {
        source: review.source,
        checksum: review.checksum,
        counts: review.counts,
        rows: review.rows,
        previewToken: previewToken(review.checksum, req.user.email),
        expiresInSeconds: 900,
      },
    });
  } catch (error) {
    return next(error);
  }
});

router.post("/apply", async (req, res, next) => {
  try {
    const review = await reviewRequest(req.body);
    if (!validPreviewToken(req.body?.previewToken, review.checksum, req.user.email)) {
      return res.status(400).json({ error: "Preview is missing, expired, or does not match this feed" });
    }

    const previous = await AffiliateLinkImportBatch.findOne({ checksum: review.checksum }).lean();
    if (previous) return res.json({ data: previous, replay: true });

    const accepted = review.rows.filter((row) => row.status === "accepted");
    if (!accepted.length) {
      return res.status(400).json({ error: "The reviewed feed has no exact catalog matches to apply" });
    }

    const verifiedAt = new Date();
    await RetailerProductMapping.bulkWrite(accepted.map((row) => ({
      updateOne: {
        filter: { source: review.source, sourceItemId: row.link.asin },
        update: {
          $set: {
            category: row.match.category,
            componentId: row.match.id,
            componentName: row.match.name,
            sourceTitle: row.match.name,
            sourceUrl: row.link.source_url,
            relationshipType: "affiliate_link",
            matchMethod: "manual",
            confidence: 1,
            active: true,
            lastSeenAt: verifiedAt,
            verifiedAt,
          },
          $setOnInsert: { createdBy: req.user.email },
        },
        upsert: true,
      },
    })));

    let batch;
    try {
      batch = await AffiliateLinkImportBatch.create({
        checksum: review.checksum,
        source: review.source,
        importedBy: req.user.email,
        counts: { ...review.counts, applied: accepted.length },
      });
    } catch (error) {
      if (error?.code !== 11000) throw error;
      batch = await AffiliateLinkImportBatch.findOne({ checksum: review.checksum }).lean();
      if (!batch) throw error;
      return res.json({ data: batch, replay: true });
    }

    return res.status(201).json({ data: batch, replay: false });
  } catch (error) {
    return next(error);
  }
});

module.exports = router;
