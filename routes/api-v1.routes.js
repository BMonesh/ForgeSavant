const express = require("express");
const mongoose = require("mongoose");
const Processor = require("../models/processor.model");
const GraphicsCard = require("../models/graphicsCard.model");
const Motherboard = require("../models/motherboard.model");
const RAM = require("../models/ram.model");
const Storage = require("../models/storage.model");
const SMPS = require("../models/smps.model");
const Cabinet = require("../models/cabinet.model");
const RetailerProductMapping = require("../models/retailerProductMapping.model");
const { evaluateCompatibility } = require("../services/compatibility.service");
const { estimatePerformance } = require("../services/analytics.service");
const { presentCatalogItem, summarizePricing } = require("../services/catalog-provenance.service");
const adminOfferRoutes = require("./admin-offers.routes");
const adminAnalyticsRoutes = require("./admin-analytics.routes");
const adminContentRoutes = require("./admin-content.routes");

const router = express.Router();

router.use("/admin/offers", adminOfferRoutes);
router.use("/admin/analytics", adminAnalyticsRoutes);
router.use("/admin/content", adminContentRoutes);

const catalogModels = {
  processors: Processor,
  gpus: GraphicsCard,
  motherboards: Motherboard,
  ram: RAM,
  storage: Storage,
  powerSupplies: SMPS,
  cabinets: Cabinet,
};

const componentModelByKey = {
  processor: Processor,
  motherboard: Motherboard,
  gpu: GraphicsCard,
  ram: RAM,
  smps: SMPS,
  cabinet: Cabinet,
  primaryStorage: Storage,
  secondaryStorage: Storage,
};

router.get("/catalog", async (req, res, next) => {
  try {
    const rawEntries = await Promise.all(
      Object.entries(catalogModels).map(async ([key, Model]) => [key, await Model.find().lean()])
    );
    const freshnessHours = Math.max(1, Number(process.env.CATALOG_FRESHNESS_HOURS) || 24);
    const now = new Date();
    const entries = rawEntries.map(([key, items]) => [
      key,
      items.map((item) => presentCatalogItem(item, now, freshnessHours)),
    ]);
    const data = Object.fromEntries(entries);
    const counts = Object.fromEntries(entries.map(([key, items]) => [key, items.length]));
    const pricing = summarizePricing(entries.flatMap(([, items]) => items));

    res.json({
      data,
      meta: {
        counts,
        total: Object.values(counts).reduce((sum, count) => sum + count, 0),
        currency: "INR",
        pricingStatus: pricing.live > 0 ? "mixed" : "planning-data",
        pricing,
        freshnessHours,
        generatedAt: new Date().toISOString(),
      },
    });
  } catch (error) {
    next(error);
  }
});

router.get("/catalog/:category/:id", async (req, res, next) => {
  try {
    const Model = catalogModels[req.params.category];
    if (!Model) return res.status(404).json({ error: "Catalog category not found" });
    if (!mongoose.isValidObjectId(req.params.id)) return res.status(400).json({ error: "Invalid component id" });

    const [item, mappings] = await Promise.all([
      Model.findById(req.params.id).lean(),
      RetailerProductMapping.find({
        category: req.params.category,
        componentId: req.params.id,
        active: true,
      }).select("source sourceItemId sourceTitle matchMethod confidence lastSeenAt -_id").lean(),
    ]);
    if (!item) return res.status(404).json({ error: "Component not found" });

    const freshnessHours = Math.max(1, Number(process.env.CATALOG_FRESHNESS_HOURS) || 24);
    const priceHistory = [...(item.priceHistory || [])]
      .sort((left, right) => new Date(right.observedAt || right.recordedAt) - new Date(left.observedAt || left.recordedAt));
    return res.json({
      data: {
        ...presentCatalogItem(item, new Date(), freshnessHours),
        priceHistory,
        retailerMappings: mappings,
      },
    });
  } catch (error) {
    return next(error);
  }
});

router.post("/compatibility/evaluate", async (req, res, next) => {
  try {
    const ids = req.body?.componentIds || {};
    const invalidKey = Object.entries(ids).find(([, id]) => id && !mongoose.isValidObjectId(id));
    if (invalidKey) {
      return res.status(400).json({ error: `Invalid component id for ${invalidKey[0]}` });
    }

    const components = Object.fromEntries(
      await Promise.all(
        Object.entries(componentModelByKey).map(async ([key, Model]) => [
          key,
          ids[key] ? await Model.findById(ids[key]).lean() : null,
        ])
      )
    );

    const missing = Object.entries(ids)
      .filter(([, id]) => Boolean(id))
      .filter(([key]) => !components[key])
      .map(([key]) => key);
    if (missing.length) {
      return res.status(404).json({ error: "One or more components were not found", missing });
    }

    return res.json(evaluateCompatibility(components));
  } catch (error) {
    return next(error);
  }
});

router.post("/analytics/estimate", async (req, res, next) => {
  try {
    const ids = req.body?.componentIds || {};
    const invalidKey = ["processor", "gpu"].find((key) => ids[key] && !mongoose.isValidObjectId(ids[key]));
    if (invalidKey) return res.status(400).json({ error: `Invalid component id for ${invalidKey}` });
    if (!ids.processor || !ids.gpu) {
      return res.status(400).json({ error: "Processor and GPU component ids are required" });
    }

    const [processor, gpu] = await Promise.all([
      Processor.findById(ids.processor).lean(),
      GraphicsCard.findById(ids.gpu).lean(),
    ]);
    const missing = [!processor && "processor", !gpu && "gpu"].filter(Boolean);
    if (missing.length) return res.status(404).json({ error: "Analytics components not found", missing });

    return res.json(estimatePerformance(processor, gpu));
  } catch (error) {
    return next(error);
  }
});

module.exports = router;
