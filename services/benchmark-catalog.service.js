const path = require("node:path");
// Same file-first, database-fallback read the data-health console uses: the
// deployed API has no analytics directory, so without the published copy the
// build review's performance panel would always report evidence unavailable.
const { loadReport } = require("./data-quality.service");

const defaultPath = path.join(__dirname, "..", "data-pipeline", "analytics", "benchmark_catalog_summary.json");
const allowedCategories = new Set(["processors", "gpus"]);

/**
 * A page a person can read for the query a score came from.
 *
 * sourceRecordUrl is kept untouched as provenance: it is the exact request the
 * pipeline made, and for Blender Open Data that request asks for
 * response_type=datatables, which returns raw JSON. Linking people there opened
 * an unreadable data dump. The same query without that parameter renders
 * Blender's own results table.
 */
const readableSourceUrl = (url) => {
  try {
    const parsed = new URL(url);
    if (parsed.hostname === "opendata.blender.org") parsed.searchParams.delete("response_type");
    return parsed.toString();
  } catch {
    return url || "";
  }
};

const readBenchmarkCatalog = async ({ category } = {}, summaryPath = process.env.BENCHMARK_SUMMARY_PATH || defaultPath) => {
  if (category && !allowedCategories.has(category)) {
    const error = new Error("Benchmark category must be processors or gpus");
    error.statusCode = 400;
    throw error;
  }
  const parsed = await loadReport("benchmark_catalog_summary", summaryPath);
  if (parsed?.schemaVersion !== "1.0" || !Array.isArray(parsed.records)) {
    const error = new Error("Benchmark summary has an unsupported schema");
    error.statusCode = 503;
    throw error;
  }
  const records = (category ? parsed.records.filter((row) => row.category === category) : parsed.records)
    .map((row) => ({ ...row, sourcePageUrl: readableSourceUrl(row.sourceRecordUrl) }));
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

module.exports = { readBenchmarkCatalog, readableSourceUrl };
