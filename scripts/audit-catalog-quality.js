require("dotenv").config();
const mongoose = require("mongoose");
const Processor = require("../models/processor.model");
const GraphicsCard = require("../models/graphicsCard.model");
const Motherboard = require("../models/motherboard.model");
const RAM = require("../models/ram.model");
const Storage = require("../models/storage.model");
const SMPS = require("../models/smps.model");
const Cabinet = require("../models/cabinet.model");
const { saves2: Saves } = require("../models/saves.model");
const RetailerProductMapping = require("../models/retailerProductMapping.model");
const { normalizedIdentityName } = require("../services/catalog-identity.service");

const catalogModels = {
  processors: Processor,
  gpus: GraphicsCard,
  motherboards: Motherboard,
  ram: RAM,
  storage: Storage,
  powerSupplies: SMPS,
  cabinets: Cabinet,
};

const buildReferenceFields = {
  processors: { id: "processor", name: "cpu" },
  gpus: { id: "gpu", name: "gpu" },
  motherboards: { id: "motherboard", name: "motherboard" },
  ram: { id: "ram", name: "ram" },
  powerSupplies: { id: "smps", name: "powerSupply" },
  cabinets: { id: "cabinet", name: "cabinet" },
  storage: [
    { id: "primaryStorage", name: "primaryStorage" },
    { id: "secondaryStorage", name: "secondaryStorage" },
  ],
};

const audit = async () => {
  if (!process.env.URI) throw new Error("URI is required");
  await mongoose.connect(process.env.URI);
  const collectionEntries = await Promise.all(Object.entries(catalogModels).map(async ([category, Model]) => [category, await Model.find().lean()]));
  const collections = Object.fromEntries(collectionEntries);
  const all = collectionEntries.flatMap(([category, rows]) => rows.map((row) => ({ category, ...row })));
  const duplicateGroups = [];
  for (const [category, rows] of collectionEntries) {
    const groups = new Map();
    for (const row of rows) {
      const key = normalizedIdentityName(row.name);
      groups.set(key, [...(groups.get(key) || []), row]);
    }
    for (const [key, group] of groups) {
      if (group.length > 1) duplicateGroups.push({ category, normalizedName: key, records: group.map(({ _id, name }) => ({ id: String(_id), name })) });
    }
  }

  const mappings = await RetailerProductMapping.find().lean();
  const orphanMappings = mappings.filter((mapping) => !collections[mapping.category]?.some((item) => String(item._id) === String(mapping.componentId)));
  const saves = await Saves.find().lean();
  const orphanBuildReferences = [];
  for (const savedBuild of saves) {
    for (const [category, rawFields] of Object.entries(buildReferenceFields)) {
      for (const fields of Array.isArray(rawFields) ? rawFields : [rawFields]) {
        const componentId = savedBuild.componentIds?.[fields.id];
        const componentName = savedBuild[fields.name];
        const exists = componentId
          ? collections[category].some((item) => String(item._id) === String(componentId))
          : collections[category].some((item) => item.name === componentName);
        if (!exists) orphanBuildReferences.push({ buildId: String(savedBuild._id), category, componentId: componentId || null, componentName });
      }
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    counts: Object.fromEntries(collectionEntries.map(([category, rows]) => [category, rows.length])),
    total: all.length,
    completeness: {
      canonicalIdentity: all.filter((item) => item.identity?.canonicalKey).length,
      manufacturerPartNumber: all.filter((item) => item.identity?.manufacturerPartNumber).length,
      verifiedManufacturerPartNumber: all.filter((item) => item.identity?.manufacturerPartNumber && item.identity?.manufacturerPartNumberSourceUrl && item.identity?.manufacturerPartNumberVerifiedAt).length,
      priceHistory: all.filter((item) => item.priceHistory?.length).length,
      provenance: all.filter((item) => item.provenance?.source && item.provenance?.collected_at).length,
      sourceUrl: all.filter((item) => item.provenance?.source_url).length,
    },
    pricingStatus: {
      live: all.filter((item) => item.provenance?.data_status === "live").length,
      sample: all.filter((item) => item.provenance?.data_status !== "live").length,
    },
    duplicateGroups,
    retailerMappings: { total: mappings.length, manual: mappings.filter((mapping) => mapping.matchMethod === "manual").length, orphaned: orphanMappings },
    savedBuilds: { total: saves.length, orphanedReferences: orphanBuildReferences },
  };
  console.log(JSON.stringify(report, null, 2));
  if (process.argv.includes("--strict")) {
    const failures = [];
    for (const field of ["canonicalIdentity", "manufacturerPartNumber", "verifiedManufacturerPartNumber", "priceHistory"]) {
      if (report.completeness[field] !== report.total) failures.push(`${field}: ${report.completeness[field]}/${report.total}`);
    }
    if (report.duplicateGroups.length) failures.push(`${report.duplicateGroups.length} duplicate identity groups`);
    if (report.retailerMappings.orphaned.length) failures.push(`${report.retailerMappings.orphaned.length} orphaned retailer mappings`);
    if (report.savedBuilds.orphanedReferences.length) failures.push(`${report.savedBuilds.orphanedReferences.length} orphaned saved-build references`);
    if (process.env.REQUIRE_LIVE_PRICING === "1" && report.pricingStatus.live === 0) failures.push("no live retailer pricing observations");
    if (failures.length) throw new Error(`Catalog release gate failed: ${failures.join("; ")}`);
  }
  await mongoose.disconnect();
};

audit().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch {}
  process.exit(1);
});
