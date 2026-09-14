const test = require("node:test");
const assert = require("node:assert/strict");
const { findOrphanBuildReferences } = require("../scripts/audit-catalog-quality");

const catalog = {
  processors: [{ _id: "cpu-1", name: "Ryzen 5" }],
  gpus: [{ _id: "gpu-1", name: "RTX 4060" }],
  motherboards: [{ _id: "mb-1", name: "B550M" }],
  ram: [{ _id: "ram-1", name: "16GB DDR4" }],
  storage: [{ _id: "ssd-1", name: "1TB NVMe" }, { _id: "ssd-2", name: "1TB SATA" }],
  powerSupplies: [{ _id: "psu-1", name: "650W" }],
  cabinets: [{ _id: "case-1", name: "Airflow" }],
};

const build = (overrides = {}) => ({
  _id: "build-1",
  cpu: "Ryzen 5", gpu: "RTX 4060", motherboard: "B550M", ram: "16GB DDR4",
  primaryStorage: "1TB NVMe", secondaryStorage: "", powerSupply: "650W", cabinet: "Airflow",
  componentIds: {
    processor: "cpu-1", gpu: "gpu-1", motherboard: "mb-1", ram: "ram-1",
    primaryStorage: "ssd-1", smps: "psu-1", cabinet: "case-1",
  },
  ...overrides,
});

test("a build saved without the optional secondary drive has no orphaned references", () => {
  assert.deepEqual(findOrphanBuildReferences([build()], catalog), []);
});

test("a secondary drive that was chosen is still checked", () => {
  const saved = build({ secondaryStorage: "1TB SATA", componentIds: { ...build().componentIds, secondaryStorage: "gone" } });
  const orphans = findOrphanBuildReferences([saved], catalog);
  assert.equal(orphans.length, 1);
  assert.equal(orphans[0].componentId, "gone");
});

test("a required part with no reference at all is still an orphan", () => {
  const saved = build({ primaryStorage: "", componentIds: { ...build().componentIds, primaryStorage: undefined } });
  const orphans = findOrphanBuildReferences([saved], catalog);
  assert.deepEqual(orphans.map((row) => row.category), ["storage"]);
});

test("a reference to a deleted product is an orphan", () => {
  const saved = build({ componentIds: { ...build().componentIds, processor: "cpu-deleted" } });
  assert.deepEqual(findOrphanBuildReferences([saved], catalog).map((row) => row.category), ["processors"]);
});
