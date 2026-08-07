const mongoose = require("mongoose");

const analyticsEventSchema = new mongoose.Schema({
  schemaVersion: { type: String, enum: ["1.0"], default: "1.0" },
  eventType: { type: String, enum: ["build_saved", "build_updated"], required: true, index: true },
  subjectHash: { type: String, required: true, index: true, select: false },
  buildHash: { type: String, required: true },
  componentIds: {
    processor: { type: mongoose.Schema.Types.ObjectId, ref: "processors", required: true },
    motherboard: { type: mongoose.Schema.Types.ObjectId, ref: "motherboards", required: true },
    gpu: { type: mongoose.Schema.Types.ObjectId, ref: "graphicscards", required: true },
    primaryStorage: { type: mongoose.Schema.Types.ObjectId, ref: "storages", required: true },
    secondaryStorage: { type: mongoose.Schema.Types.ObjectId, ref: "storages" },
    ram: { type: mongoose.Schema.Types.ObjectId, ref: "rams", required: true },
    smps: { type: mongoose.Schema.Types.ObjectId, ref: "powersupplies", required: true },
    cabinet: { type: mongoose.Schema.Types.ObjectId, ref: "cabinets", required: true },
  },
  buildTotal: { type: Number, min: 0, required: true },
  currency: { type: String, enum: ["INR"], default: "INR" },
  compatibilityStatus: { type: String, enum: ["compatible"], required: true },
  compatibilityEngineVersion: { type: String, required: true },
  analyticsModelVersion: { type: String, required: true },
  occurredAt: { type: Date, default: Date.now, index: true },
}, {
  timestamps: { createdAt: "recordedAt", updatedAt: false },
  versionKey: false,
});

analyticsEventSchema.index({ eventType: 1, occurredAt: -1 });

module.exports = mongoose.model("analytics_events", analyticsEventSchema);
