require("dotenv").config();
const fs = require("node:fs/promises");
const path = require("node:path");
const mongoose = require("mongoose");
const { reviewContent, applyReviewedContent } = require("../services/content-import.service");
const ContentImportBatch = require("../models/contentImportBatch.model");
const Processor = require("../models/processor.model");
const GraphicsCard = require("../models/graphicsCard.model");
const Motherboard = require("../models/motherboard.model");
const RAM = require("../models/ram.model");
const Storage = require("../models/storage.model");
const SMPS = require("../models/smps.model");
const Cabinet = require("../models/cabinet.model");

const catalogModels = { processors: Processor, gpus: GraphicsCard, motherboards: Motherboard, ram: RAM, storage: Storage, powerSupplies: SMPS, cabinets: Cabinet };
const args = new Set(process.argv.slice(2));
const applying = args.has("--apply");
const feedPath = path.join(__dirname, "..", "data-pipeline", "authorized_product_content_feed.json");

const main = async () => {
  if (!process.env.URI) throw new Error("URI is required");
  const payload = JSON.parse(await fs.readFile(feedPath, "utf8"));
  if (payload.schema_version !== "1.0" || payload.source !== "open_icecat") throw new Error("Unsupported product-content feed");
  await mongoose.connect(process.env.URI, {
    serverSelectionTimeoutMS: 15000,
    readPreference: applying ? "primary" : "secondaryPreferred",
  });
  const review = await reviewContent({ observations: payload.observations, catalogModels });
  const result = {
    mode: applying ? "apply" : "preview",
    checksum: review.checksum,
    counts: review.counts,
    exceptions: review.rows.filter((row) => row.status !== "accepted").map((row) => ({
      index: row.index,
      status: row.status,
      manufacturerPartNumber: row.observation.manufacturer_part_number,
      reason: row.reason || row.errors?.join("; "),
    })),
  };
  if (applying) {
    const operatorEmail = String(process.env.PIPELINE_OPERATOR_EMAIL || process.env.ADMIN_EMAILS || "").split(",")[0].trim().toLowerCase();
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(operatorEmail)) throw new Error("Set PIPELINE_OPERATOR_EMAIL or ADMIN_EMAILS before applying content evidence");
    const applied = await applyReviewedContent({ review, catalogModels, batchModel: ContentImportBatch, operatorEmail });
    result.replay = applied.replay;
    result.applied = applied.batch.counts.applied;
  }
  console.log(JSON.stringify(result, null, 2));
};

main()
  .catch((error) => { console.error(error.message); process.exitCode = 1; })
  .finally(async () => { await mongoose.disconnect(); });
