require("dotenv").config();
const fs = require("node:fs");
const path = require("node:path");
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
const {
  normalizedIdentityName,
  canonicalKey,
  uniqueAliases,
  initialPriceHistory,
  chooseDuplicateKeeper,
  specificationsEqual,
} = require("../services/catalog-identity.service");

const APPLY = process.argv.includes("--apply");
const catalogModels = {
  processors: Processor,
  gpus: GraphicsCard,
  motherboards: Motherboard,
  ram: RAM,
  storage: Storage,
  powerSupplies: SMPS,
  cabinets: Cabinet,
};
const saveReferences = {
  processors: [{ name: "cpu", id: "componentIds.processor" }],
  gpus: [{ name: "gpu", id: "componentIds.gpu" }],
  motherboards: [{ name: "motherboard", id: "componentIds.motherboard" }],
  ram: [{ name: "ram", id: "componentIds.ram" }],
  storage: [
    { name: "primaryStorage", id: "componentIds.primaryStorage" },
    { name: "secondaryStorage", id: "componentIds.secondaryStorage" },
  ],
  powerSupplies: [{ name: "powerSupply", id: "componentIds.smps" }],
  cabinets: [{ name: "cabinet", id: "componentIds.cabinet" }],
};

const backupCatalog = async () => {
  const backup = { createdAt: new Date().toISOString(), collections: {}, saves: await Saves.find().lean(), mappings: await RetailerProductMapping.find().lean() };
  for (const [category, Model] of Object.entries(catalogModels)) backup.collections[category] = await Model.find().lean();
  const backupDir = path.join(__dirname, "..", "data-pipeline", "backups");
  fs.mkdirSync(backupDir, { recursive: true });
  const filename = `catalog-identity-${new Date().toISOString().replace(/[:.]/g, "-")}.json`;
  const backupPath = path.join(backupDir, filename);
  fs.writeFileSync(backupPath, JSON.stringify(backup, null, 2), "utf8");
  return backupPath;
};

const migrate = async () => {
  if (!process.env.URI) throw new Error("URI is required");
  await mongoose.connect(process.env.URI);
  const report = { mode: APPLY ? "apply" : "dry-run", startedAt: new Date().toISOString(), before: 0, after: 0, enriched: 0, duplicates: [], conflicts: [], backupPath: null };
  if (APPLY) report.backupPath = await backupCatalog();

  for (const [category, Model] of Object.entries(catalogModels)) {
    const components = await Model.find().lean();
    report.before += components.length;
    const groups = new Map();
    for (const component of components) {
      const key = normalizedIdentityName(component.name);
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(component);
    }

    for (const group of groups.values()) {
      const keeper = chooseDuplicateKeeper(group);
      const duplicates = group.filter((component) => String(component._id) !== String(keeper._id));
      const conflicting = duplicates.filter((component) => !specificationsEqual(component.specifications, keeper.specifications));
      if (conflicting.length) {
        report.conflicts.push({ category, names: group.map((component) => component.name), reason: "specifications differ" });
      }
      const safeDuplicates = duplicates.filter((component) => specificationsEqual(component.specifications, keeper.specifications));
      const aliases = uniqueAliases(keeper.identity?.aliases || [], group.map((component) => component.name));
      const priceHistory = keeper.priceHistory?.length
        ? [...keeper.priceHistory]
        : [initialPriceHistory(keeper)];
      for (const duplicate of safeDuplicates) {
        if (duplicate.priceHistory?.length) priceHistory.push(...duplicate.priceHistory);
        else priceHistory.push(initialPriceHistory(duplicate));
      }
      const identity = {
        canonicalKey: canonicalKey(category, keeper.name),
        manufacturerPartNumber: keeper.identity?.manufacturerPartNumber || "",
        manufacturerPartNumberSourceUrl: keeper.identity?.manufacturerPartNumberSourceUrl || "",
        manufacturerPartNumberVerifiedAt: keeper.identity?.manufacturerPartNumberVerifiedAt || null,
        aliases,
        lifecycleStatus: keeper.identity?.lifecycleStatus || "unknown",
      };
      report.enriched += 1;

      if (APPLY) {
        await Model.updateOne({ _id: keeper._id }, { $set: { identity, priceHistory } });
      }

      for (const duplicate of safeDuplicates) {
        report.duplicates.push({ category, removedId: String(duplicate._id), removedName: duplicate.name, keeperId: String(keeper._id), keeperName: keeper.name });
        if (!APPLY) continue;
        for (const reference of saveReferences[category]) {
          await Saves.updateMany({ [reference.name]: duplicate.name }, { $set: { [reference.name]: keeper.name } });
          await Saves.updateMany({ [reference.id]: duplicate._id }, { $set: { [reference.id]: keeper._id, [reference.name]: keeper.name } });
        }
        await RetailerProductMapping.updateMany(
          { category, componentId: duplicate._id },
          { $set: { componentId: keeper._id, componentName: keeper.name } }
        );
        await Model.deleteOne({ _id: duplicate._id });
      }
    }
    report.after += components.length - report.duplicates.filter((item) => item.category === category).length;
  }

  report.completedAt = new Date().toISOString();
  console.log(JSON.stringify(report, null, 2));
  await mongoose.disconnect();
};

migrate().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch {}
  process.exit(1);
});
