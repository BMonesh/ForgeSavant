const express = require("express");
const jwt = require("jsonwebtoken");
const { authenticate } = require("../middleware/auth");
const { requireAdmin } = require("../services/admin-access.service");
const { reviewOffers } = require("../services/offer-import.service");
const OfferImportBatch = require("../models/offerImportBatch.model");
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

const loadMappings = async (source, offers) => {
  const sourceName = String(source || "").trim().toLowerCase();
  const sourceItemIds = (Array.isArray(offers) ? offers : [])
    .map((offer) => String(offer?.source_item_id || offer?.source_sku || "").trim())
    .filter(Boolean);
  return RetailerProductMapping.find({ source: sourceName, sourceItemId: { $in: sourceItemIds }, active: true }).lean();
};

router.use(authenticate, requireAdmin);

const createPreviewToken = (review, email) => jwt.sign(
  { kind: "offer-import-preview", checksum: review.checksum, email },
  process.env.JWT_SECRET,
  { algorithm: "HS256", expiresIn: "15m" }
);

const verifyPreviewToken = (token, checksum, email) => {
  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET, { algorithms: ["HS256"] });
    return decoded.kind === "offer-import-preview" && decoded.checksum === checksum && decoded.email === email;
  } catch {
    return false;
  }
};

router.get("/status", (req, res) => {
  res.json({ isAdmin: true, email: req.user.email, maxRecords: 500 });
});

router.get("/history", async (req, res, next) => {
  try {
    const batches = await OfferImportBatch.find().sort({ createdAt: -1 }).limit(20).lean();
    res.json({ data: batches });
  } catch (error) {
    next(error);
  }
});

router.post("/preview", async (req, res, next) => {
  try {
    const mappings = await loadMappings(req.body?.source, req.body?.offers);
    const review = await reviewOffers({
      source: req.body?.source,
      offers: req.body?.offers,
      catalogModels,
      mappings,
      resolutions: req.body?.resolutions || [],
    });
    res.json({
      data: {
        source: review.source,
        checksum: review.checksum,
        counts: review.counts,
        rows: review.rows,
        previewToken: createPreviewToken(review, req.user.email),
        expiresInSeconds: 900,
      },
    });
  } catch (error) {
    next(error);
  }
});

router.post("/apply", async (req, res, next) => {
  try {
    const mappings = await loadMappings(req.body?.source, req.body?.offers);
    const review = await reviewOffers({
      source: req.body?.source,
      offers: req.body?.offers,
      catalogModels,
      mappings,
      resolutions: req.body?.resolutions || [],
    });
    if (!verifyPreviewToken(req.body?.previewToken, review.checksum, req.user.email)) {
      return res.status(400).json({ error: "Preview is missing, expired, or does not match this feed" });
    }

    const previous = await OfferImportBatch.findOne({ checksum: review.checksum }).lean();
    if (previous) {
      return res.json({ data: previous, replay: true });
    }

    const accepted = review.rows.filter((row) => row.status === "accepted");
    if (!accepted.length) {
      return res.status(400).json({ error: "The reviewed feed has no verified offers to apply" });
    }
    const operationsByCategory = accepted.reduce((groups, row) => {
      const provenance = {
        source: review.source,
        source_url: row.offer.source_url,
        source_item_id: row.offer.source_item_id,
        currency: row.offer.currency,
        availability: row.offer.availability,
        collected_at: row.offer.observed_at,
        data_status: "live",
        import_checksum: review.checksum,
        imported_by: req.user.email,
      };
      const update = { price: row.offer.price, provenance };
      if (row.offer.image_url && catalogModels[row.match.category].schema.path("image_url")) {
        update.image_url = row.offer.image_url;
      }
      groups[row.match.category] ||= [];
      groups[row.match.category].push({
        updateOne: {
          filter: {
            _id: row.match.id,
            "priceHistory.importChecksum": { $ne: review.checksum },
          },
          update: {
            $set: update,
            $push: {
              priceHistory: {
                price: row.offer.price,
                currency: row.offer.currency,
                availability: row.offer.availability,
                source: review.source,
                sourceUrl: row.offer.source_url,
                sourceItemId: row.offer.source_item_id,
                observedAt: row.offer.observed_at,
                importChecksum: review.checksum,
                recordedAt: new Date(),
              },
            },
          },
        },
      });
      return groups;
    }, {});

    for (const [category, operations] of Object.entries(operationsByCategory)) {
      if (operations.length) await catalogModels[category].bulkWrite(operations);
    }
    if (accepted.length) {
      await RetailerProductMapping.bulkWrite(accepted.map((row) => ({
        updateOne: {
          filter: { source: review.source, sourceItemId: row.offer.source_item_id },
          update: {
            $set: {
              category: row.match.category,
              componentId: row.match.id,
              componentName: row.match.name,
              sourceTitle: row.offer.name,
              matchMethod: row.matchMethod === "manual" ? "manual" : "automatic",
              confidence: row.score,
              active: true,
              lastSeenAt: row.offer.observed_at,
            },
            $setOnInsert: { createdBy: req.user.email },
          },
          upsert: true,
        },
      })));
    }
    let batch;
    try {
      batch = await OfferImportBatch.create({
        checksum: review.checksum,
        source: review.source,
        importedBy: req.user.email,
        counts: { ...review.counts, applied: accepted.length },
        categories: Object.keys(operationsByCategory),
      });
    } catch (error) {
      if (error?.code !== 11000) throw error;
      batch = await OfferImportBatch.findOne({ checksum: review.checksum }).lean();
      if (!batch) throw error;
      return res.json({ data: batch, replay: true });
    }

    return res.status(201).json({ data: batch, replay: false });
  } catch (error) {
    return next(error);
  }
});

module.exports = router;
