const fs = require("fs");
const path = require("path");
const mongoose = require("mongoose");
require("dotenv").config();

const Processor = require("../models/processor.model");
const GraphicsCard = require("../models/graphicsCard.model");
const Motherboard = require("../models/motherboard.model");
const RAM = require("../models/ram.model");
const Storage = require("../models/storage.model");
const SMPS = require("../models/smps.model");
const Cabinet = require("../models/cabinet.model");

const mongoUri = process.env.URI || "mongodb://127.0.0.1:27017/forgesavant";
const cleanedDir = path.join(__dirname, "..", "data-pipeline", "cleaned_data");

const parseCsv = (filePath) => {
  const content = fs.readFileSync(filePath, "utf8").replace(/^\uFEFF/, "");
  const [headerLine, ...lines] = content.trim().split(/\r?\n/);
  const headers = headerLine.split(",");

  return lines.map((line) => {
    const values = line.split(",");
    return headers.reduce((row, header, index) => {
      row[header] = values[index];
      return row;
    }, {});
  });
};

const toNumber = (value) => Number(value || 0);

const upsertMany = async (Model, documents) => {
  for (const doc of documents) {
    await Model.updateOne({ name: doc.name }, { $set: doc }, { upsert: true });
  }

  return documents.length;
};

const buildProcessors = () =>
  parseCsv(path.join(cleanedDir, "processors_cleaned.csv")).map((row) => ({
    name: row.name,
    type: row.type,
    manufacturer: row.manufacturer,
    specifications: {
      cores: toNumber(row.cores),
      threads: toNumber(row.threads),
      base_clock: row.base_clock,
      boost_clock: row.boost_clock,
      cache: row.cache,
      socket: row.socket,
      tdp: row.tdp,
    },
    price: toNumber(row.price),
  }));

const buildGpus = () =>
  parseCsv(path.join(cleanedDir, "gpus_cleaned.csv")).map((row) => ({
    name: row.name,
    type: row.type,
    manufacturer: row.manufacturer,
    specifications: {
      core_count: toNumber(row.core_count),
      base_clock: row.base_clock,
      boost_clock: row.boost_clock,
      memory: row.memory,
      tdp: row.tdp,
    },
    price: toNumber(row.price),
  }));

const buildMotherboards = () =>
  parseCsv(path.join(cleanedDir, "motherboards_cleaned.csv")).map((row) => ({
    name: row.name,
    type: row.type,
    manufacturer: row.manufacturer,
    specifications: {
      socket: row.socket,
      chipset: row.chipset,
      memory_type: row.memory_type,
      form_factor: row.form_factor,
      memory_slots: toNumber(row.memory_slots),
      max_memory: row.max_memory,
      pcie_slots: toNumber(row.pcie_slots),
      sata_ports: toNumber(row.sata_ports),
      m2_slots: toNumber(row.m2_slots),
      lan: row.lan,
      usb_ports: row.usb_ports,
    },
    price: toNumber(row.price),
    provenance: { source: row.source, currency: "INR", availability: "unknown", data_status: "sample" },
  }));

const buildRam = () =>
  parseCsv(path.join(cleanedDir, "ram_cleaned.csv")).map((row) => ({
    name: row.name,
    type: row.type,
    manufacturer: row.manufacturer,
    specifications: {
      capacity: row.capacity,
      type: row.ram_type,
      speed: row.speed,
      cas_latency: toNumber(row.cas_latency),
      voltage: row.voltage,
      rgb: String(row.rgb).toLowerCase() === "true",
    },
    price: toNumber(row.price),
  }));

const storage = [
  {
    name: "Samsung 980 1TB NVMe SSD",
    type: "SSD",
    manufacturer: "Samsung",
    specifications: {
      capacity: "1TB",
      interface: "NVMe",
      form_factor: "M.2 2280",
      speed: "3500 MB/s",
      technology: "TLC NAND",
      encryption: "AES 256-bit",
      tbw: "600TB",
      warranty: "5 years",
    },
    price: 6499,
    image_url: "https://placehold.co/600x400/202a25/d39b00?text=NVMe+SSD",
  },
  {
    name: "WD Blue SN580 1TB NVMe SSD",
    type: "SSD",
    manufacturer: "Western Digital",
    specifications: {
      capacity: "1TB",
      interface: "NVMe",
      form_factor: "M.2 2280",
      speed: "4150 MB/s",
      technology: "TLC NAND",
      encryption: "None",
      tbw: "600TB",
      warranty: "5 years",
    },
    price: 5899,
    image_url: "https://placehold.co/600x400/202a25/d39b00?text=NVMe+SSD",
  },
  {
    name: "Crucial BX500 1TB SATA SSD",
    type: "SSD",
    manufacturer: "Crucial",
    specifications: {
      capacity: "1TB",
      interface: "SATA",
      form_factor: "2.5 inch",
      speed: "540 MB/s",
      technology: "3D NAND",
      encryption: "None",
      tbw: "360TB",
      warranty: "3 years",
    },
    price: 4999,
    image_url: "https://placehold.co/600x400/202a25/d39b00?text=SATA+SSD",
  },
  {
    name: "Seagate Barracuda 2TB HDD",
    type: "HDD",
    manufacturer: "Seagate",
    specifications: {
      capacity: "2TB",
      interface: "SATA",
      form_factor: "3.5 inch",
      speed: "7200 RPM",
      technology: "CMR",
      encryption: "None",
      tbw: "N/A",
      warranty: "2 years",
    },
    price: 5299,
    image_url: "https://placehold.co/600x400/202a25/d39b00?text=SATA+Storage",
  },
];

const powerSupplies = [450, 550, 650, 750, 850, 1000].map((wattage) => ({
  name: `Cooler Master MWE ${wattage}W Bronze`,
  type: "Power Supply",
  manufacturer: "Cooler Master",
  specifications: {
    wattage: `${wattage}W`,
    efficiency: "80+ Bronze",
    modular: wattage >= 750,
    certifications: ["80+ Bronze", "ATX"],
    fan_size: "120mm",
    dimensions: "140 x 150 x 86 mm",
    weight: "1.8kg",
  },
  price: Math.round(wattage * 8.5),
}));

const cabinets = [
  {
    name: "ForgeSavant Airflow ATX Case",
    type: "Cabinet",
    manufacturer: "ForgeSavant",
    specifications: {
      form_factor: "Mid Tower",
      motherboard_support: "ATX, Micro-ATX, Mini-ITX",
      fan_support: "6x 120mm",
      radiator_support: "360mm front, 240mm top",
      gpu_clearance: "360mm",
      cpu_cooler_clearance: "165mm",
      storage: "2x 2.5 inch, 2x 3.5 inch",
      dimensions: "450 x 210 x 470 mm",
    },
    price: 5999,
    image_url: "https://placehold.co/600x400/202a25/d39b00?text=ATX+Cabinet",
  },
  {
    name: "ForgeSavant Compact Micro-ATX Case",
    type: "Cabinet",
    manufacturer: "ForgeSavant",
    specifications: {
      form_factor: "Mini Tower",
      motherboard_support: "Micro-ATX, Mini-ITX",
      fan_support: "4x 120mm",
      radiator_support: "240mm front",
      gpu_clearance: "320mm",
      cpu_cooler_clearance: "155mm",
      storage: "2x 2.5 inch, 1x 3.5 inch",
      dimensions: "390 x 205 x 410 mm",
    },
    price: 4299,
    image_url: "https://placehold.co/600x400/202a25/d39b00?text=Micro+ATX+Cabinet",
  },
];

const main = async () => {
  await mongoose.connect(mongoUri);

  const existingRecords = (await Promise.all([
    Processor, GraphicsCard, Motherboard, RAM, Storage, SMPS, Cabinet,
  ].map((Model) => Model.estimatedDocumentCount()))).reduce((sum, count) => sum + count, 0);
  if (existingRecords > 0 && process.env.SEED_ALLOW_EXISTING !== "1") {
    throw new Error(`Refusing to seed a non-empty catalog (${existingRecords} records). Use an empty database or explicitly set SEED_ALLOW_EXISTING=1.`);
  }

  const results = {
    processors: await upsertMany(Processor, buildProcessors()),
    gpus: await upsertMany(GraphicsCard, buildGpus()),
    motherboards: await upsertMany(Motherboard, buildMotherboards()),
    ram: await upsertMany(RAM, buildRam()),
    storage: await upsertMany(Storage, storage),
    powerSupplies: await upsertMany(SMPS, powerSupplies),
    cabinets: await upsertMany(Cabinet, cabinets),
  };

  console.log("Seed complete:");
  console.table(results);
  await mongoose.connection.close();
};

main().catch(async (err) => {
  console.error("Seed failed:", err);
  await mongoose.connection.close();
  process.exit(1);
});
