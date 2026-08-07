const mongoose = require('mongoose');
const { componentMetadataFields } = require('./componentMetadata.schema');

const GraphicsCardSchema = new mongoose.Schema({
  _id: {
    type: mongoose.Schema.Types.ObjectId,
    required: true,
  },
  name: {
    type: String,
    required: true,
    unique: true,
    index: true,
  },
  type: {
    type: String,
    required: true,
  },
  manufacturer: {
    type: String,
    required: true,
  },
  specifications: {
    core_count: {
      type: Number,
      required: true,
    },
    base_clock: {
      type: String,
      required: true,
    },
    boost_clock: {
      type: String,
      required: true,
    },
    memory: {
      type: String,
      required: true,
    },
    tdp: {
      type: String,
      required: true,
    },
  },
  price: {
    type: Number,
    required: true,
  },
  provenance: { type: mongoose.Schema.Types.Mixed },
  ...componentMetadataFields,
});

// Create the GraphicsCard model from the schema
const GraphicsCard = mongoose.model('graphiccards', GraphicsCardSchema);

module.exports = GraphicsCard;
