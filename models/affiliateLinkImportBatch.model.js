const mongoose = require("mongoose");

const affiliateLinkImportBatchSchema = new mongoose.Schema({
  checksum: { type: String, required: true, unique: true },
  source: { type: String, required: true, default: "amazon.in" },
  importedBy: { type: String, required: true },
  counts: {
    received: { type: Number, required: true },
    accepted: { type: Number, required: true },
    ambiguous: { type: Number, required: true },
    unmatched: { type: Number, required: true },
    rejected: { type: Number, required: true },
    applied: { type: Number, required: true },
  },
}, { timestamps: true });

module.exports = mongoose.model("affiliate_link_import_batches", affiliateLinkImportBatchSchema);
