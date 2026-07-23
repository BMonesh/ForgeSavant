const mongoose = require('mongoose');
const { componentMetadataFields } = require('./componentMetadata.schema');

// Define the schema for the Motherboard model
const MotherboardSchema = new mongoose.Schema({
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
    socket: {
      type: String,
      required: true,
    },
    chipset: {
      type: String,
      required: true,
    },
    memory_type: {
      type: String,
      required: false,
    },
    form_factor: {
      type: String,
      required: true,
    },
    memory_slots: {
      type: Number,
      required: true,
    },
    max_memory: {
      type: String,
      required: true,
    },
    pcie_slots: {
      type: Number,
      required: true,
    },
    sata_ports: {
      type: Number,
      required: true,
    },
    m2_slots: {
      type: Number,
      required: true,
    },
    lan: {
      type: String,
      required: true,
    },
    usb_ports: {
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

// Create the Motherboard model from the schema
const Motherboard = mongoose.model('motherboards', MotherboardSchema);

module.exports = Motherboard;
