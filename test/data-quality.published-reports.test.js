const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const mongoose = require("mongoose");

const db = require("../db");
const service = require("../services/data-quality.service");

const temporaryFile = async (payload) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "forgesavant-reports-"));
  const file = path.join(directory, "report.json");
  await fs.writeFile(file, JSON.stringify(payload), "utf8");
  return file;
};

const missingFile = () => path.join(os.tmpdir(), "forgesavant-absent", `${Date.now()}.json`);

/**
 * Replace the database seam the service reads through. The service asks
 * db.isConnected() first and only then touches mongoose.connection.db, so
 * stubbing both keeps these tests free of a live MongoDB.
 */
const withDatabase = async (documents, run) => {
  const originalIsConnected = db.isConnected;
  const descriptor = Object.getOwnPropertyDescriptor(mongoose, "connection");
  const queries = [];

  db.isConnected = () => documents !== null;
  Object.defineProperty(mongoose, "connection", {
    configurable: true,
    value: {
      db: {
        collection(name) {
          return {
            async findOne(query) {
              queries.push({ collection: name, query });
              return (documents || []).find((row) => row.name === query.name) || null;
            },
          };
        },
      },
    },
  });

  try {
    return await run(queries);
  } finally {
    db.isConnected = originalIsConnected;
    if (descriptor) Object.defineProperty(mongoose, "connection", descriptor);
  }
};

test("a local analytics file stays authoritative over the published copy", async () => {
  const file = await temporaryFile({ schemaVersion: "1.0", source: "file" });
  await withDatabase([{ name: "pipeline_status", payload: { schemaVersion: "1.0", source: "database" } }], async (queries) => {
    const report = await service.loadReport("pipeline_status", file);
    assert.equal(report.source, "file");
    assert.equal(queries.length, 0, "the database should not be consulted when the file exists");
  });
});

test("a missing file falls back to the report the pipeline published", async () => {
  await withDatabase([{ name: "pipeline_status", payload: { schemaVersion: "1.0", source: "database" } }], async () => {
    const report = await service.loadReport("pipeline_status", missingFile());
    assert.equal(report.source, "database");
  });
});

test("the fallback asks for the requested report by name", async () => {
  await withDatabase([{ name: "catalog_coverage_queue", payload: { schemaVersion: "1.0" } }], async (queries) => {
    await service.loadReport("catalog_coverage_queue", missingFile());
    assert.deepEqual(queries[0], {
      collection: "pipeline_reports",
      query: { name: "catalog_coverage_queue" },
    });
  });
});

test("a report that is neither on disk nor published raises ENOENT", async () => {
  await withDatabase([], async () => {
    await assert.rejects(
      () => service.loadReport("pipeline_status", missingFile()),
      (error) => error.code === "ENOENT",
    );
  });
});

test("a disconnected database never reaches for a published report", async () => {
  await withDatabase(null, async (queries) => {
    await assert.rejects(() => service.loadReport("pipeline_status", missingFile()));
    assert.equal(queries.length, 0);
  });
});

test("readPublishedReport returns null while the database is unavailable", async () => {
  await withDatabase(null, async () => {
    assert.equal(await service.readPublishedReport("pipeline_status"), null);
  });
});

test("operational status is served from the published report when no file exists", async () => {
  const published = {
    schemaVersion: "1.0",
    status: "succeeded",
    startedAt: "2026-09-02T00:00:00+00:00",
    completedAt: "2026-09-02T00:01:00+00:00",
    stages: [{ name: "analytics_build", status: "succeeded", durationSeconds: 1.2, exitCode: 0 }],
  };
  await withDatabase([{ name: "pipeline_status", payload: published }], async () => {
    const status = await service.readOperationalStatus(missingFile());
    assert.equal(status.status, "succeeded");
    assert.equal(status.stages[0].name, "analytics_build");
  });
});

test("the coverage work queue is served from the published report", async () => {
  const published = {
    schemaVersion: "1.0",
    generatedAt: "2026-09-02T00:00:00+00:00",
    records: [
      { status: "manufacturer_ready", category: "gpus", catalogName: "Example GPU", priority: 90 },
      { status: "covered", category: "processors", catalogName: "Covered CPU", priority: 0 },
    ],
  };
  await withDatabase([{ name: "catalog_coverage_queue", payload: published }], async () => {
    const queue = await service.readCoverageWorkQueue(missingFile());
    assert.equal(queue.totalGaps, 1);
    assert.equal(queue.records[0].catalogName, "Example GPU");
  });
});

test("the summary still reports 503 when nothing is available anywhere", async () => {
  await withDatabase([], async () => {
    await assert.rejects(
      () => service.readDataQualitySummary(missingFile()),
      (error) => error.statusCode === 503,
    );
  });
});

test("a published summary satisfies the data-quality endpoint", async () => {
  const published = {
    schemaVersion: "1.0",
    generatedAt: new Date().toISOString(),
    grain: "one observation",
    catalog: { verifiedProducts: 58, observedProducts: 14 },
    pipeline: { received: 204, accepted: 150, duplicates: 54, quarantined: 0 },
    quality: { identityCompletenessRate: 1 },
    categories: { processors: 13 },
  };
  await withDatabase([{ name: "data_quality_summary", payload: published }], async () => {
    const summary = await service.readDataQualitySummary(missingFile());
    assert.equal(summary.status, "healthy");
    assert.equal(summary.catalog.verifiedProducts, 58);
    assert.equal(summary.catalog.coverageRate, 14 / 58);
  });
});
