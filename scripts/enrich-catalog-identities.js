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
const processors = require("../data-pipeline/verified_identity/processors.json");
const gpus = require("../data-pipeline/verified_identity/gpus.json");
const motherboards = require("../data-pipeline/verified_identity/motherboards.json");
const ram = require("../data-pipeline/verified_identity/ram.json");
const storage = require("../data-pipeline/verified_identity/storage.json");
const powerSupplies = require("../data-pipeline/verified_identity/power-supplies.json");
const cabinets = require("../data-pipeline/verified_identity/cabinets.json");
const { canonicalKey, uniqueAliases } = require("../services/catalog-identity.service");

const APPLY = process.argv.includes("--apply");
const groups = [
  { category: "processors", Model: Processor, saveField: "cpu", componentIdField: "processor", records: processors },
  { category: "gpus", Model: GraphicsCard, saveField: "gpu", componentIdField: "gpu", records: gpus },
  { category: "motherboards", Model: Motherboard, saveField: "motherboard", componentIdField: "motherboard", records: motherboards },
  { category: "ram", Model: RAM, saveField: "ram", componentIdField: "ram", records: ram },
  { category: "storage", Model: Storage, saveFields: ["primaryStorage", "secondaryStorage"], componentIdFields: ["primaryStorage", "secondaryStorage"], records: storage },
  { category: "powerSupplies", Model: SMPS, saveField: "powerSupply", componentIdField: "smps", records: powerSupplies },
  { category: "cabinets", Model: Cabinet, saveField: "cabinet", componentIdField: "cabinet", records: cabinets },
];
const allowedManufacturerHosts = new Set([
  "www.amd.com",
  "www.intel.com",
  "www.sapphiretech.com",
  "www.zotac.com",
  "www.asus.com",
  "rog.asus.com",
  "www.asrock.com",
  "www.gigabyte.com",
  "www.msi.com",
  "www.corsair.com",
  "www.gskill.com",
  "www.kingston.com",
  "www.teamgroupinc.com",
  "www.samsung.com",
  "documents.westerndigital.com",
  "www.crucial.com",
  "eu.crucial.com",
  "www.seagate.com",
  "www.coolermaster.com",
  "support.nzxt.com",
]);

const validateManifest = (category, records) => {
  const errors = [];
  const names = new Set();
  const partNumbers = new Set();
  records.forEach((record, index) => {
    const name = String(record.name || "").trim();
    const partNumber = String(record.manufacturerPartNumber || "").trim().toUpperCase();
    let source;
    try { source = new URL(record.sourceUrl); } catch { errors.push(`${category} row ${index + 1}: sourceUrl is invalid`); }
    if (!name) errors.push(`${category} row ${index + 1}: name is required`);
    if (!/^[A-Z0-9/_-]{6,48}$/.test(partNumber)) errors.push(`${category} row ${index + 1}: manufacturerPartNumber is invalid`);
    if (source && (source.protocol !== "https:" || !allowedManufacturerHosts.has(source.hostname))) errors.push(`${category} row ${index + 1}: source must be an approved manufacturer HTTPS page`);
    if (names.has(name.toLowerCase())) errors.push(`${category} row ${index + 1}: duplicate product name`);
    if (partNumbers.has(partNumber)) errors.push(`${category} row ${index + 1}: duplicate manufacturer part number`);
    names.add(name.toLowerCase());
    partNumbers.add(partNumber);
  });
  return errors;
};

const run = async () => {
  if (!process.env.URI) throw new Error("URI is required");
  const manifestErrors = groups.flatMap(({ category, records }) => validateManifest(category, records));
  if (manifestErrors.length) throw new Error(`Identity manifest failed validation:\n${manifestErrors.join("\n")}`);
  await mongoose.connect(process.env.URI);
  const report = { mode: APPLY ? "apply" : "dry-run", matched: 0, changed: 0, unchanged: 0, missing: [], conflicts: [], categories: {}, backupPath: null };
  const pending = [];
  const backups = { createdAt: new Date().toISOString(), collections: {}, saves: await Saves.find().lean() };

  for (const group of groups) {
    const items = await group.Model.find().lean();
    backups.collections[group.category] = items;
    const byName = new Map(items.flatMap((item) => [[item.name.toLowerCase(), item], ...(item.identity?.aliases || []).map((alias) => [alias.toLowerCase(), item])]));
    const categoryReport = { manifest: group.records.length, matched: 0, changed: 0, unchanged: 0 };
    const operations = [];
    const saveRenames = [];

    for (const record of group.records) {
      const lookupNames = [record.currentName, record.name].filter(Boolean).map((name) => name.toLowerCase());
      const item = lookupNames.map((name) => byName.get(name)).find(Boolean);
      if (!item) { report.missing.push({ category: group.category, name: record.currentName || record.name }); continue; }
      report.matched += 1;
      categoryReport.matched += 1;
      const existing = String(item.identity?.manufacturerPartNumber || "").toUpperCase();
      if (existing && existing !== record.manufacturerPartNumber) {
        report.conflicts.push({ category: group.category, name: item.name, existing, proposed: record.manufacturerPartNumber });
        continue;
      }
      const proposedSpecifications = { ...(item.specifications || {}), ...(record.specifications || {}) };
      const identity = {
        ...(item.identity || {}),
        canonicalKey: canonicalKey(group.category, record.name),
        manufacturerPartNumber: record.manufacturerPartNumber,
        manufacturerPartNumberSourceUrl: record.sourceUrl,
        manufacturerPartNumberVerifiedAt: item.identity?.manufacturerPartNumberVerifiedAt || new Date(),
        aliases: uniqueAliases(item.identity?.aliases || [], item.name, record.currentName),
      };
      const unchanged = item.name === record.name
        && (!record.manufacturer || item.manufacturer === record.manufacturer)
        && existing === record.manufacturerPartNumber
        && item.identity?.manufacturerPartNumberSourceUrl === record.sourceUrl
        && JSON.stringify(item.specifications || {}) === JSON.stringify(proposedSpecifications);
      if (unchanged) { report.unchanged += 1; categoryReport.unchanged += 1; continue; }
      report.changed += 1;
      categoryReport.changed += 1;
      operations.push({ updateOne: { filter: { _id: item._id }, update: { $set: { name: record.name, manufacturer: record.manufacturer || item.manufacturer, specifications: proposedSpecifications, identity } } } });
      if (item.name !== record.name) saveRenames.push({ oldName: item.name, newName: record.name, componentId: item._id });
    }
    report.categories[group.category] = categoryReport;
    pending.push({ ...group, operations, saveRenames });
  }

  if (report.missing.length || report.conflicts.length) throw new Error(`Identity enrichment blocked:\n${JSON.stringify(report, null, 2)}`);
  if (APPLY && report.changed) {
    const backupDir = path.join(__dirname, "..", "data-pipeline", "backups");
    fs.mkdirSync(backupDir, { recursive: true });
    report.backupPath = path.join(backupDir, `verified-identities-${new Date().toISOString().replace(/[:.]/g, "-")}.json`);
    fs.writeFileSync(report.backupPath, JSON.stringify(backups, null, 2), "utf8");
    for (const group of pending) {
      if (group.operations.length) await group.Model.bulkWrite(group.operations);
      for (const rename of group.saveRenames) {
        const saveFields = group.saveFields || [group.saveField];
        const componentIdFields = group.componentIdFields || [group.componentIdField];
        for (let index = 0; index < saveFields.length; index += 1) {
          const saveField = saveFields[index];
          const componentIdField = componentIdFields[index];
          await Saves.updateMany({ [saveField]: rename.oldName }, { $set: { [saveField]: rename.newName } });
          await Saves.updateMany({ [`componentIds.${componentIdField}`]: rename.componentId }, { $set: { [saveField]: rename.newName } });
        }
      }
    }
  }
  console.log(JSON.stringify(report, null, 2));
  await mongoose.disconnect();
};

run().catch(async (error) => {
  console.error(error.message || error);
  try { await mongoose.disconnect(); } catch {}
  process.exit(1);
});
