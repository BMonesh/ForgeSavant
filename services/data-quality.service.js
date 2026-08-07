const fs = require("node:fs/promises");
const path = require("node:path");

const defaultSummaryPath = path.join(__dirname, "..", "data-pipeline", "analytics", "data_quality_summary.json");
const defaultStatusPath = path.join(__dirname, "..", "data-pipeline", "analytics", "pipeline_status.json");
const defaultReadinessPath = path.join(__dirname, "..", "data-pipeline", "analytics", "model_readiness_summary.json");
const defaultRetailSnapshotPath = path.join(__dirname, "..", "data-pipeline", "analytics", "retail_snapshot_report.json");
const defaultCoverageQueuePath = path.join(__dirname, "..", "data-pipeline", "analytics", "catalog_coverage_queue.json");

const ratio = (numerator, denominator) => denominator > 0 ? numerator / denominator : null;

const readOperationalStatus = async (statusPath = process.env.PIPELINE_STATUS_PATH || defaultStatusPath) => {
  try {
    const status = JSON.parse(await fs.readFile(statusPath, "utf8"));
    if (status?.schemaVersion !== "1.0" || !["running", "succeeded", "failed"].includes(status.status)) return null;
    return {
      status: status.status,
      startedAt: status.startedAt || null,
      completedAt: status.completedAt || null,
      stages: Array.isArray(status.stages) ? status.stages.map((stage) => ({
        name: stage.name,
        status: stage.status,
        durationSeconds: stage.durationSeconds,
        exitCode: stage.exitCode,
      })) : [],
      error: status.error || null,
    };
  } catch (error) {
    if (error.code === "ENOENT") return null;
    return null;
  }
};

const readModelReadiness = async (readinessPath = process.env.MODEL_READINESS_PATH || defaultReadinessPath) => {
  try {
    const parsed = JSON.parse(await fs.readFile(readinessPath, "utf8"));
    if (parsed?.schemaVersion !== "1.0" || !parsed.dataset || !Array.isArray(parsed.uses)) return null;
    return {
      generatedAt: parsed.generatedAt,
      dataset: parsed.dataset,
      checks: parsed.checks || {},
      uses: parsed.uses,
      requiredNextEvidence: Array.isArray(parsed.requiredNextEvidence) ? parsed.requiredNextEvidence : [],
    };
  } catch (_error) {
    return null;
  }
};

const readRetailSnapshot = async (snapshotPath = process.env.RETAIL_SNAPSHOT_PATH || defaultRetailSnapshotPath) => {
  try {
    const parsed = JSON.parse(await fs.readFile(snapshotPath, "utf8"));
    if (parsed?.schemaVersion !== "1.0") return null;
    const skipReasons = (Array.isArray(parsed.skipped) ? parsed.skipped : []).reduce((counts, row) => {
      const reason = row?.reason || "Unspecified validation failure";
      counts[reason] = (counts[reason] || 0) + 1;
      return counts;
    }, {});
    return {
      snapshotAt: parsed.snapshotAt || null,
      scannedComponents: Number(parsed.scannedComponents) || 0,
      scannedPriceHistoryEntries: Number(parsed.scannedPriceHistoryEntries) || 0,
      eligibleOffers: Number(parsed.eligibleOffers) || 0,
      accepted: Number(parsed.accepted) || 0,
      duplicates: Number(parsed.duplicates) || 0,
      quarantined: Number(parsed.quarantined) || 0,
      skippedEntries: Object.values(skipReasons).reduce((total, count) => total + count, 0),
      skipReasons,
    };
  } catch (_error) {
    return null;
  }
};

const readCoverageWorkQueue = async (queuePath = process.env.COVERAGE_QUEUE_PATH || defaultCoverageQueuePath, limit = 12) => {
  try {
    const parsed = JSON.parse(await fs.readFile(queuePath, "utf8"));
    if (parsed?.schemaVersion !== "1.0" || !Array.isArray(parsed.records)) return null;
    const records = parsed.records
      .filter((row) => row?.status !== "covered")
      .slice(0, Math.max(1, Math.min(50, Number(limit) || 12)))
      .map((row) => ({
        priority: Number(row.priority) || 0,
        status: row.status,
        category: row.category,
        catalogName: row.catalogName,
        manufacturer: row.manufacturer,
        manufacturerPartNumber: row.manufacturerPartNumber,
        manufacturerSourceUrl: row.manufacturerSourceUrl,
        latestIcecatStatus: row.latestIcecatStatus,
        gapReason: row.gapReason,
      }));
    return { generatedAt: parsed.generatedAt || null, totalGaps: parsed.records.filter((row) => row?.status !== "covered").length, records };
  } catch (_error) {
    return null;
  }
};

const readDataQualitySummary = async (summaryPath = process.env.DATA_QUALITY_SUMMARY_PATH || defaultSummaryPath) => {
  let parsed;
  try {
    parsed = JSON.parse(await fs.readFile(summaryPath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") {
      const unavailable = new Error("Analytics summary is unavailable; run npm run analytics:build");
      unavailable.statusCode = 503;
      throw unavailable;
    }
    throw error;
  }

  if (parsed?.schemaVersion !== "1.0" || !parsed.catalog || !parsed.pipeline || !parsed.quality || !parsed.categories) {
    const invalid = new Error("Analytics summary has an unsupported schema");
    invalid.statusCode = 503;
    throw invalid;
  }

  const generatedAt = new Date(parsed.generatedAt);
  if (Number.isNaN(generatedAt.getTime())) {
    const invalid = new Error("Analytics summary has an invalid generation time");
    invalid.statusCode = 503;
    throw invalid;
  }
  const freshnessHours = Math.max(1, Number(process.env.PIPELINE_FRESHNESS_HOURS) || 168);
  const ageHours = Math.max(0, (Date.now() - generatedAt.getTime()) / 3_600_000);
  const coverageRate = ratio(parsed.catalog.observedProducts, parsed.catalog.verifiedProducts);
  const validationPassRate = ratio(parsed.pipeline.accepted + parsed.pipeline.duplicates, parsed.pipeline.received);
  const operational = await readOperationalStatus();
  const modelReadiness = await readModelReadiness();
  const retailSnapshot = await readRetailSnapshot();
  const coverageWorkQueue = await readCoverageWorkQueue();
  const status = operational?.status === "failed" || parsed.pipeline.quarantined > 0
    ? "attention"
    : ageHours > freshnessHours ? "stale" : "healthy";

  return {
    schemaVersion: parsed.schemaVersion,
    generatedAt: parsed.generatedAt,
    grain: parsed.grain,
    status,
    freshness: { ageHours, thresholdHours: freshnessHours },
    catalog: { ...parsed.catalog, coverageRate },
    pipeline: { ...parsed.pipeline, validationPassRate },
    retail: parsed.retail || {
      priceObservations: 0,
      productsWithPriceHistory: 0,
      currentOffers: 0,
      productsWithCurrentOffers: 0,
      retailers: 0,
      validPriceRate: null,
      latestObservedAt: null,
    },
    coverageQueue: parsed.coverageQueue || null,
    outcomes: parsed.outcomes || {
      observations: 0,
      pseudonymousSubjects: 0,
      builds: 0,
      saved: 0,
      updated: 0,
      observationDates: 0,
    },
    benchmarks: parsed.benchmarks || {
      observations: 0,
      currentObservations: 0,
      products: 0,
      metrics: 0,
      sources: 0,
      observationDates: 0,
    },
    operational,
    modelReadiness,
    retailSnapshot,
    coverageWorkQueue,
    quality: parsed.quality,
    categories: parsed.categories,
    caveats: Array.isArray(parsed.caveats) ? parsed.caveats : [],
    definitions: {
      catalogCoverage: "Distinct verified catalog products with at least one accepted normalized observation / verified catalog products.",
      identityCompleteness: "Accepted observations containing both manufacturer and manufacturer part number / accepted observations.",
      gtinCoverage: "Accepted observations containing at least one GTIN / accepted observations.",
      quarantineRate: "Rejected observations written to quarantine / observations received across ingestion runs.",
      validationPassRate: "Accepted plus known duplicate observations / observations received. Duplicate observations are valid, idempotent reprocessing outcomes.",
      priceHistoryCoverage: "Verified catalog products with at least one authorized retailer price observation / verified catalog products.",
    },
  };
};

module.exports = { readDataQualitySummary, readOperationalStatus, readModelReadiness, readRetailSnapshot, readCoverageWorkQueue };
