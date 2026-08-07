const mongoose = require('mongoose');
const { componentMetadataFields } = require('./componentMetadata.schema');

const PowerSupplySchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
    unique: true,
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
    wattage: {
      type: String,
      required: true,
    },
    efficiency: {
      type: String,
      required: true,
    },
    modular: {
      type: Boolean,
      required: true,
    },
    certifications: [{
      type: String,
      required: true,
    }],
    fan_size: {
      type: String,
      required: true,
    },
    dimensions: {
      type: String,
      required: true,
    },
    weight: {
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

// Create the PowerSupply model from the schema
const PowerSupply = mongoose.model('powersupplies', PowerSupplySchema);

module.exports = PowerSupply;
