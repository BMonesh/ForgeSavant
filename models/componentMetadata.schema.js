const mongoose = require("mongoose");

const identitySchema = new mongoose.Schema({
  canonicalKey: { type: String, index: true },
  manufacturerPartNumber: { type: String, default: "" },
  manufacturerPartNumberSourceUrl: { type: String, default: "" },
  manufacturerPartNumberVerifiedAt: { type: Date },
  aliases: { type: [String], default: [] },
  lifecycleStatus: {
    type: String,
    enum: ["active", "discontinued", "unknown"],
    default: "unknown",
  },
}, { _id: false });

const priceHistorySchema = new mongoose.Schema({
  price: { type: Number, required: true },
  currency: { type: String, default: "INR" },
  availability: { type: String, default: "unknown" },
  source: { type: String, required: true },
  sourceUrl: { type: String, default: "" },
  sourceItemId: { type: String, default: "" },
  observedAt: { type: Date },
  importChecksum: { type: String, default: "" },
  recordedAt: { type: Date, default: Date.now },
}, { _id: false });

const productContentEvidenceSchema = new mongoose.Schema({
  observationId: { type: String, required: true },
  source: { type: String, required: true },
  sourceTier: { type: String, default: "" },
  sourceProductId: { type: String, required: true },
  manufacturerPartNumber: { type: String, required: true },
  sourceReportedPartNumber: { type: String, default: "" },
  gtins: { type: [String], default: [] },
  specifications: { type: mongoose.Schema.Types.Mixed, default: {} },
  imageUrl: { type: String, default: "" },
  manufacturerUrl: { type: String, default: "" },
  sourceRecordUrl: { type: String, default: "" },
  observedAt: { type: Date, required: true },
  ingestedAt: { type: Date, required: true },
  rawSha256: { type: String, required: true },
  importChecksum: { type: String, required: true },
  importedBy: { type: String, required: true },
  importedAt: { type: Date, default: Date.now },
}, { _id: false });

const componentMetadataFields = {
  identity: { type: identitySchema, default: () => ({}) },
  priceHistory: { type: [priceHistorySchema], default: [] },
  productContentEvidence: { type: [productContentEvidenceSchema], default: [] },
};

module.exports = { componentMetadataFields };
