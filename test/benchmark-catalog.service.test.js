const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { readBenchmarkCatalog } = require("../services/benchmark-catalog.service");

test("filters benchmark records without exposing internal paths", async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "forgesavant-benchmarks-"));
  const summaryPath = path.join(directory, "summary.json");
  fs.writeFileSync(summaryPath, JSON.stringify({
    schemaVersion: "1.0", generatedAt: "2026-08-07T00:00:00Z", grain: "current aggregate",
    caveats: ["Compare within category."], records: [
      { category: "processors", catalogName: "CPU", metricValue: 100 },
      { category: "gpus", catalogName: "GPU", metricValue: 200 },
    ],
  }));
  try {
    const result = await readBenchmarkCatalog({ category: "gpus" }, summaryPath);
    assert.equal(result.records.length, 1);
    assert.equal(result.records[0].catalogName, "GPU");
    assert.deepEqual(result.counts, { total: 1, processors: 0, gpus: 1 });
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("rejects unsupported benchmark categories", async () => {
  await assert.rejects(() => readBenchmarkCatalog({ category: "ram" }, "unused"), /processors or gpus/);
});
