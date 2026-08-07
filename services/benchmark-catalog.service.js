const fs = require("node:fs/promises");
const path = require("node:path");

const defaultPath = path.join(__dirname, "..", "data-pipeline", "analytics", "benchmark_catalog_summary.json");
const allowedCategories = new Set(["processors", "gpus"]);

const readBenchmarkCatalog = async ({ category } = {}, summaryPath = process.env.BENCHMARK_SUMMARY_PATH || defaultPath) => {
  if (category && !allowedCategories.has(category)) {
    const error = new Error("Benchmark category must be processors or gpus");
    error.statusCode = 400;
    throw error;
  }
  const parsed = JSON.parse(await fs.readFile(summaryPath, "utf8"));
  if (parsed?.schemaVersion !== "1.0" || !Array.isArray(parsed.records)) {
    const error = new Error("Benchmark summary has an unsupported schema");
    error.statusCode = 503;
    throw error;
  }
  const records = category ? parsed.records.filter((row) => row.category === category) : parsed.records;
  return {
    schemaVersion: parsed.schemaVersion,
    generatedAt: parsed.generatedAt,
    grain: parsed.grain,
    caveats: parsed.caveats || [],
    records,
    counts: {
      total: records.length,
      processors: records.filter((row) => row.category === "processors").length,
      gpus: records.filter((row) => row.category === "gpus").length,
    },
  };
};

module.exports = { readBenchmarkCatalog };
