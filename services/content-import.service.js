const crypto = require("node:crypto");

const CATEGORY_MODELS = {
  processors: "processors",
  gpus: "gpus",
  motherboards: "motherboards",
  ram: "ram",
  storage: "storage",
  power_supplies: "powerSupplies",
  cabinets: "cabinets",
};
const HTTPS_URL = /^https:\/\//i;
const SHA256 = /^[a-f0-9]{64}$/i;
const FORBIDDEN_OBJECT_KEYS = new Set(["__proto__", "prototype", "constructor"]);

const text = (value) => String(value ?? "").trim();
const validDate = (value) => {
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const normalizeSpecifications = (value, errors) => {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    errors.push("specifications must be an object");
    return {};
  }
  const entries = Object.entries(value);
  if (entries.length > 250) errors.push("specifications cannot contain more than 250 fields");
  const normalized = {};
  for (const [rawKey, rawValue] of entries.slice(0, 250)) {
    const key = text(rawKey);
    if (!/^[a-z0-9_]{1,80}$/.test(key) || FORBIDDEN_OBJECT_KEYS.has(key)) {
      errors.push(`invalid specification key: ${key || "empty"}`);
      continue;
    }
    if (!["string", "number", "boolean"].includes(typeof rawValue) && rawValue !== null) {
      errors.push(`specification ${key} must be a scalar value`);
      continue;
    }
    if (typeof rawValue === "string" && rawValue.length > 1000) {
      errors.push(`specification ${key} is too long`);
      continue;
    }
    normalized[key] = rawValue;
  }
  return normalized;
};

const normalizeObservation = (raw, index, now = new Date()) => {
  const errors = [];
  const warnings = [];
  const category = text(raw?.catalog_category);
  const observedAt = validDate(raw?.observed_at);
  const ingestedAt = validDate(raw?.ingested_at);
  const imageUrl = text(raw?.image_url);
  const manufacturerUrl = text(raw?.manufacturer_url);
  const sourceRecordUrl = text(raw?.source_record_url);
  const normalized = {
    schema_version: text(raw?.schema_version),
    observation_id: text(raw?.observation_id).toLowerCase(),
    observation_kind: text(raw?.observation_kind),
    source: text(raw?.source).toLowerCase(),
    source_tier: text(raw?.source_tier).toLowerCase(),
    source_product_id: text(raw?.source_product_id),
    catalog_category: category,
    catalog_name: text(raw?.catalog_name),
    manufacturer: text(raw?.manufacturer),
    manufacturer_part_number: text(raw?.manufacturer_part_number).toUpperCase(),
    source_reported_part_number: text(raw?.source_reported_part_number).toUpperCase(),
    name: text(raw?.name),
    gtins: Array.isArray(raw?.gtins) ? [...new Set(raw.gtins.map(text).filter(Boolean))].slice(0, 20) : [],
    specifications: normalizeSpecifications(raw?.specifications, errors),
    image_url: HTTPS_URL.test(imageUrl) ? imageUrl : "",
    manufacturer_url: HTTPS_URL.test(manufacturerUrl) ? manufacturerUrl : "",
    source_record_url: HTTPS_URL.test(sourceRecordUrl) ? sourceRecordUrl : "",
    observed_at: observedAt?.toISOString() || text(raw?.observed_at),
    ingested_at: ingestedAt?.toISOString() || text(raw?.ingested_at),
    raw_sha256: text(raw?.raw_sha256).toLowerCase(),
  };

  if (normalized.schema_version !== "1.0") errors.push("schema_version must be 1.0");
  if (!SHA256.test(normalized.observation_id)) errors.push("observation_id must be a SHA-256 value");
  if (normalized.observation_kind !== "product_content") errors.push("observation_kind must be product_content");
  if (normalized.source !== "open_icecat") errors.push("source must be open_icecat");
  if (!CATEGORY_MODELS[category]) errors.push("catalog_category is not supported");
  if (!normalized.source_product_id) errors.push("source_product_id is required");
  if (!normalized.manufacturer || !normalized.manufacturer_part_number) errors.push("manufacturer identity is required");
  if (normalized.manufacturer_part_number.length > 80 || normalized.source_reported_part_number.length > 80) errors.push("part numbers cannot exceed 80 characters");
  if (!normalized.name || !normalized.catalog_name) errors.push("source and catalog names are required");
  if (!observedAt || !ingestedAt) errors.push("observed_at and ingested_at must be ISO timestamps");
  if (observedAt && observedAt.getTime() > now.getTime() + 15 * 60 * 1000) errors.push("observed_at cannot be in the future");
  for (const [field, value] of [["image_url", imageUrl], ["manufacturer_url", manufacturerUrl], ["source_record_url", sourceRecordUrl]]) {
    if (value && !HTTPS_URL.test(value)) warnings.push(`${field} was omitted because it is not an HTTPS URL`);
  }
  if (!SHA256.test(normalized.raw_sha256)) errors.push("raw_sha256 must be a SHA-256 value");
  return { index, errors: [...new Set(errors)], warnings: [...new Set(warnings)], observation: normalized };
};

const canonicalFeed = (observations) => JSON.stringify(observations.map(({ observation }) => observation));
const feedChecksum = (observations) => crypto.createHash("sha256").update(canonicalFeed(observations)).digest("hex");
const summarize = (rows) => rows.reduce((counts, row) => {
  counts[row.status] += 1;
  return counts;
}, { accepted: 0, ambiguous: 0, unmatched: 0, rejected: 0 });

const reviewContent = async ({ observations, catalogModels, now = new Date() }) => {
  if (!Array.isArray(observations) || observations.length < 1 || observations.length > 500) {
    const error = new Error("observations must contain between 1 and 500 records");
    error.statusCode = 400;
    throw error;
  }
  const normalized = observations.map((observation, index) => normalizeObservation(observation, index, now));
  const categories = [...new Set(normalized.filter((row) => !row.errors.length).map((row) => row.observation.catalog_category))];
  const catalog = Object.fromEntries(await Promise.all(categories.map(async (category) => {
    const modelKey = CATEGORY_MODELS[category];
    return [category, await catalogModels[modelKey].find().select("name identity manufacturer").lean()];
  })));
  const rows = normalized.map((row) => {
    if (row.errors.length) return { index: row.index, status: "rejected", errors: row.errors, warnings: row.warnings, observation: row.observation };
    const matches = (catalog[row.observation.catalog_category] || []).filter(
      (item) => text(item.identity?.manufacturerPartNumber).toUpperCase() === row.observation.manufacturer_part_number
    );
    if (!matches.length) return { index: row.index, status: "unmatched", reason: "No exact catalog manufacturer part number match", observation: row.observation };
    if (matches.length > 1) return { index: row.index, status: "ambiguous", reason: "Manufacturer part number matches multiple catalog records", observation: row.observation };
    return {
      index: row.index,
      status: "accepted",
      warnings: row.warnings,
      observation: row.observation,
      match: { id: String(matches[0]._id), name: matches[0].name, category: CATEGORY_MODELS[row.observation.catalog_category] },
    };
  });
  return {
    source: "open_icecat",
    checksum: feedChecksum(normalized),
    rows,
    counts: { received: observations.length, ...summarize(rows) },
  };
};

const applyReviewedContent = async ({ review, catalogModels, batchModel, operatorEmail }) => {
  const previous = await batchModel.findOne({ checksum: review.checksum }).lean();
  if (previous) return { batch: previous, replay: true };
  const accepted = review.rows.filter((row) => row.status === "accepted");
  if (!accepted.length) {
    const error = new Error("The reviewed feed has no exact identity matches to apply");
    error.statusCode = 400;
    throw error;
  }
  const operationsByCategory = accepted.reduce((groups, row) => {
    const observation = row.observation;
    groups[row.match.category] ||= [];
    groups[row.match.category].push({ updateOne: {
      filter: { _id: row.match.id, "productContentEvidence.observationId": { $ne: observation.observation_id } },
      update: { $push: { productContentEvidence: {
        observationId: observation.observation_id,
        source: observation.source,
        sourceTier: observation.source_tier,
        sourceProductId: observation.source_product_id,
        manufacturerPartNumber: observation.manufacturer_part_number,
        sourceReportedPartNumber: observation.source_reported_part_number,
        gtins: observation.gtins,
        specifications: observation.specifications,
        imageUrl: observation.image_url,
        manufacturerUrl: observation.manufacturer_url,
        sourceRecordUrl: observation.source_record_url,
        observedAt: observation.observed_at,
        ingestedAt: observation.ingested_at,
        rawSha256: observation.raw_sha256,
        importChecksum: review.checksum,
        importedBy: operatorEmail,
        importedAt: new Date(),
      } } },
    } });
    return groups;
  }, {});
  let applied = 0;
  for (const [category, operations] of Object.entries(operationsByCategory)) {
    const result = await catalogModels[category].bulkWrite(operations);
    applied += result.modifiedCount || 0;
  }
  let batch;
  try {
    batch = await batchModel.create({
      checksum: review.checksum,
      source: review.source,
      importedBy: operatorEmail,
      counts: { ...review.counts, applied },
      categories: Object.keys(operationsByCategory),
    });
  } catch (error) {
    if (error?.code !== 11000) throw error;
    batch = await batchModel.findOne({ checksum: review.checksum }).lean();
    if (!batch) throw error;
    return { batch, replay: true };
  }
  return { batch, replay: false };
};

module.exports = { normalizeObservation, reviewContent, applyReviewedContent, feedChecksum, CATEGORY_MODELS };
