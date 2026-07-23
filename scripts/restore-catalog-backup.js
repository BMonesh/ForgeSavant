const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');
require('dotenv').config();

const Processor = require('../models/processor.model');
const GraphicsCard = require('../models/graphicsCard.model');
const Motherboard = require('../models/motherboard.model');
const RAM = require('../models/ram.model');
const Storage = require('../models/storage.model');
const SMPS = require('../models/smps.model');
const Cabinet = require('../models/cabinet.model');
const { saves2: Saves } = require('../models/saves.model');
const RetailerProductMapping = require('../models/retailerProductMapping.model');

const models = {
  processors: Processor,
  gpus: GraphicsCard,
  motherboards: Motherboard,
  ram: RAM,
  storage: Storage,
  powerSupplies: SMPS,
  cabinets: Cabinet,
};

const backupArg = process.argv[2];
const confirmed = process.argv.includes('--confirm');
const mongoUri = process.env.URI || 'mongodb://127.0.0.1:27017/forgesavant';
const isLocal = /mongodb:\/\/(127\.0\.0\.1|localhost)(:\d+)?\//i.test(mongoUri);

const fail = (message) => {
  console.error(message);
  process.exitCode = 1;
};

const restoreDocuments = async (Model, records) => {
  await Model.deleteMany({});
  if (!records.length) return;
  const documents = records.map((record) => Model.hydrate(record).toObject({ depopulate: true }));
  await Model.collection.insertMany(documents, { ordered: true });
};

const run = async () => {
  if (!backupArg || !confirmed) {
    return fail('Usage: node scripts/restore-catalog-backup.js <backup.json> --confirm');
  }
  if (!isLocal) return fail('Restore refused: this command is restricted to local MongoDB');

  const backupPath = path.resolve(backupArg);
  if (!fs.existsSync(backupPath)) return fail(`Backup not found: ${backupPath}`);
  const backup = JSON.parse(fs.readFileSync(backupPath, 'utf8'));
  const missing = Object.keys(models).filter((category) => !Array.isArray(backup.collections?.[category]));
  if (missing.length || !Array.isArray(backup.saves) || !Array.isArray(backup.mappings)) {
    return fail(`Invalid backup structure${missing.length ? `; missing ${missing.join(', ')}` : ''}`);
  }

  await mongoose.connect(mongoUri);
  const recovery = { createdAt: new Date().toISOString(), collections: {}, saves: await Saves.find().lean(), mappings: await RetailerProductMapping.find().lean() };
  for (const [category, Model] of Object.entries(models)) recovery.collections[category] = await Model.find().lean();
  const recoveryDir = path.join(__dirname, '..', 'data-pipeline', 'backups');
  fs.mkdirSync(recoveryDir, { recursive: true });
  const recoveryPath = path.join(recoveryDir, `pre-restore-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
  fs.writeFileSync(recoveryPath, JSON.stringify(recovery, null, 2), 'utf8');

  for (const [category, Model] of Object.entries(models)) {
    await restoreDocuments(Model, backup.collections[category]);
  }
  await restoreDocuments(Saves, backup.saves);
  await restoreDocuments(RetailerProductMapping, backup.mappings);

  console.log(JSON.stringify({ restoredFrom: backupPath, recoveryPath, catalogRecords: Object.values(backup.collections).reduce((sum, records) => sum + records.length, 0), saves: backup.saves.length, mappings: backup.mappings.length }, null, 2));
};

run()
  .catch((error) => { console.error(error); process.exitCode = 1; })
  .finally(() => mongoose.disconnect());
