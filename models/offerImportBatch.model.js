const mongoose = require("mongoose");

const offerImportBatchSchema = new mongoose.Schema({
  checksum: { type: String, required: true, unique: true, index: true },
  source: { type: String, required: true },
  importedBy: { type: String, required: true },
  status: { type: String, enum: ["applied"], default: "applied" },
  counts: {
    received: { type: Number, required: true },
    accepted: { type: Number, required: true },
    applied: { type: Number, required: true },
    ambiguous: { type: Number, required: true },
    unmatched: { type: Number, required: true },
    rejected: { type: Number, required: true },
  },
  categories: [{ type: String }],
}, { timestamps: true });

module.exports = mongoose.model("offer_import_batches", offerImportBatchSchema);
