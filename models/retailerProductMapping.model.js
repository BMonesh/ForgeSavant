const mongoose = require("mongoose");

const retailerProductMappingSchema = new mongoose.Schema({
  source: { type: String, required: true },
  sourceItemId: { type: String, required: true },
  category: {
    type: String,
    enum: ["processors", "gpus", "motherboards", "ram", "storage", "powerSupplies", "cabinets"],
    required: true,
  },
  componentId: { type: mongoose.Schema.Types.ObjectId, required: true },
  componentName: { type: String, required: true },
  sourceTitle: { type: String, required: true },
  matchMethod: { type: String, enum: ["automatic", "manual"], required: true },
  confidence: { type: Number, min: 0, max: 1, required: true },
  active: { type: Boolean, default: true },
  createdBy: { type: String, required: true },
  lastSeenAt: { type: Date },
}, { timestamps: true });

retailerProductMappingSchema.index({ source: 1, sourceItemId: 1 }, { unique: true });
retailerProductMappingSchema.index({ category: 1, componentId: 1 });

module.exports = mongoose.model("retailer_product_mappings", retailerProductMappingSchema);
