const express = require("express");
const jwt = require("jsonwebtoken");
const { authenticate } = require("../middleware/auth");
const { requireAdmin } = require("../services/admin-access.service");
const { reviewContent, applyReviewedContent } = require("../services/content-import.service");
const ContentImportBatch = require("../models/contentImportBatch.model");
const Processor = require("../models/processor.model");
const GraphicsCard = require("../models/graphicsCard.model");
const Motherboard = require("../models/motherboard.model");
const RAM = require("../models/ram.model");
const Storage = require("../models/storage.model");
const SMPS = require("../models/smps.model");
const Cabinet = require("../models/cabinet.model");

const router = express.Router();
const catalogModels = { processors: Processor, gpus: GraphicsCard, motherboards: Motherboard, ram: RAM, storage: Storage, powerSupplies: SMPS, cabinets: Cabinet };

router.use(authenticate, requireAdmin);

const createPreviewToken = (review, email) => jwt.sign(
  { kind: "content-import-preview", checksum: review.checksum, email },
  process.env.JWT_SECRET,
  { algorithm: "HS256", expiresIn: "15m" }
);

const validPreviewToken = (token, checksum, email) => {
  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET, { algorithms: ["HS256"] });
    return decoded.kind === "content-import-preview" && decoded.checksum === checksum && decoded.email === email;
  } catch {
    return false;
  }
};

router.get("/history", async (_req, res, next) => {
  try {
    return res.json({ data: await ContentImportBatch.find().sort({ createdAt: -1 }).limit(20).lean() });
  } catch (error) {
    return next(error);
  }
});

router.post("/preview", async (req, res, next) => {
  try {
    const review = await reviewContent({ observations: req.body?.observations, catalogModels });
    return res.json({ data: {
      source: review.source,
      checksum: review.checksum,
      counts: review.counts,
      rows: review.rows,
      previewToken: createPreviewToken(review, req.user.email),
      expiresInSeconds: 900,
    } });
  } catch (error) {
    return next(error);
  }
});

router.post("/apply", async (req, res, next) => {
  try {
    const review = await reviewContent({ observations: req.body?.observations, catalogModels });
    if (!validPreviewToken(req.body?.previewToken, review.checksum, req.user.email)) {
      return res.status(400).json({ error: "Preview is missing, expired, or does not match this content feed" });
    }
    const result = await applyReviewedContent({ review, catalogModels, batchModel: ContentImportBatch, operatorEmail: req.user.email });
    return res.status(result.replay ? 200 : 201).json({ data: result.batch, replay: result.replay });
  } catch (error) {
    return next(error);
  }
});

module.exports = router;
