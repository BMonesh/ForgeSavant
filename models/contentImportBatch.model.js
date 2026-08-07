const mongoose = require("mongoose");

const contentImportBatchSchema = new mongoose.Schema({
  checksum: { type: String, required: true, unique: true, index: true },
  source: { type: String, required: true },
  importedBy: { type: String, required: true },
  counts: {
    received: { type: Number, required: true },
    accepted: { type: Number, required: true },
    ambiguous: { type: Number, required: true },
    unmatched: { type: Number, required: true },
    rejected: { type: Number, required: true },
    applied: { type: Number, required: true },
  },
  categories: { type: [String], default: [] },
}, { timestamps: true });

module.exports = mongoose.model("contentImportBatches", contentImportBatchSchema);
