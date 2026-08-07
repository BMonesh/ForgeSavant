const crypto = require("node:crypto");
const { normalizeCategory } = require("./offer-import.service");

const AMAZON_SOURCE = "amazon.in";
const ASIN_PATTERN = /^[A-Z0-9]{10}$/;
const TAG_PATTERN = /^[a-z0-9][a-z0-9-]{1,127}$/i;

const cleanText = (value) => String(value ?? "").trim();

const buildAmazonIndiaUrl = (asin, associateTag) => {
  const normalizedAsin = cleanText(asin).toUpperCase();
  const normalizedTag = cleanText(associateTag);
  if (!ASIN_PATTERN.test(normalizedAsin)) throw new Error("ASIN must contain exactly 10 letters or numbers");
  if (!TAG_PATTERN.test(normalizedTag)) throw new Error("Amazon Associate tag is not configured correctly");
  return `https://www.amazon.in/dp/${normalizedAsin}?tag=${encodeURIComponent(normalizedTag)}`;
};

const normalizeAmazonLink = (raw, index) => {
  const asin = cleanText(raw?.asin || raw?.source_item_id).toUpperCase();
  const category = normalizeCategory(raw?.category);
  const componentId = cleanText(raw?.component_id || raw?.componentId);
  const manufacturerPartNumber = cleanText(raw?.manufacturer_part_number || raw?.mpn).toUpperCase();
  const errors = [];

  if (!ASIN_PATTERN.test(asin)) errors.push("asin must contain exactly 10 letters or numbers");
  if (!category) errors.push("category is not supported");
  if (!componentId && !manufacturerPartNumber) {
    errors.push("component_id or manufacturer_part_number is required");
  }
  if (componentId && !/^[a-f0-9]{24}$/i.test(componentId)) errors.push("component_id is not a valid identifier");

  return {
    index,
    errors,
    link: {
      asin,
      category,
      component_id: componentId,
      manufacturer_part_number: manufacturerPartNumber,
    },
  };
};

const canonicalFeed = (associateTag, normalized, rows = []) => JSON.stringify({
  source: AMAZON_SOURCE,
  associateTag: cleanText(associateTag),
  links: normalized.map(({ link }) => link),
  matches: rows.map((row) => ({
    index: row.index,
    status: row.status,
    componentId: row.match?.id || "",
  })),
});

const feedChecksum = (associateTag, normalized, rows = []) => crypto
  .createHash("sha256")
  .update(canonicalFeed(associateTag, normalized, rows))
  .digest("hex");

const summarize = (rows) => rows.reduce((counts, row) => {
  counts[row.status] += 1;
  return counts;
}, { accepted: 0, ambiguous: 0, unmatched: 0, rejected: 0 });

const reviewAmazonLinks = async ({ links, associateTag, catalogModels }) => {
  if (!TAG_PATTERN.test(cleanText(associateTag))) {
    const error = new Error("AMAZON_ASSOCIATE_TAG is not configured");
    error.statusCode = 503;
    throw error;
  }
  if (!Array.isArray(links) || links.length < 1 || links.length > 500) {
    const error = new Error("links must contain between 1 and 500 records");
    error.statusCode = 400;
    throw error;
  }

  const normalized = links.map(normalizeAmazonLink);
  const duplicateAsins = new Set();
  const seenAsins = new Set();
  normalized.forEach(({ link }) => {
    if (!link.asin) return;
    if (seenAsins.has(link.asin)) duplicateAsins.add(link.asin);
    seenAsins.add(link.asin);
  });

  const categories = [...new Set(normalized.filter((row) => !row.errors.length).map((row) => row.link.category))];
  const catalog = Object.fromEntries(await Promise.all(categories.map(async (category) => [
    category,
    await catalogModels[category].find().select("_id name identity.manufacturerPartNumber").lean(),
  ])));

  const rows = normalized.map((row) => {
    if (row.errors.length) return { index: row.index, status: "rejected", errors: row.errors, link: row.link };
    if (duplicateAsins.has(row.link.asin)) {
      return { index: row.index, status: "rejected", errors: ["asin is duplicated in this feed"], link: row.link };
    }

    const candidates = catalog[row.link.category] || [];
    let matches = [];
    let matchedBy = "";
    if (row.link.component_id) {
      matches = candidates.filter((item) => String(item._id) === row.link.component_id);
      matchedBy = "component_id";
    } else {
      matches = candidates.filter(
        (item) => cleanText(item.identity?.manufacturerPartNumber).toUpperCase() === row.link.manufacturer_part_number
      );
      matchedBy = "manufacturer_part_number";
    }

    if (!matches.length) {
      return {
        index: row.index,
        status: "unmatched",
        reason: `No catalog component matched the supplied ${matchedBy}`,
        link: row.link,
      };
    }
    if (matches.length > 1) {
      return {
        index: row.index,
        status: "ambiguous",
        reason: `Multiple catalog components matched the supplied ${matchedBy}`,
        link: row.link,
        candidates: matches.map((item) => ({ id: String(item._id), name: item.name })),
      };
    }

    return {
      index: row.index,
      status: "accepted",
      matchedBy,
      link: {
        ...row.link,
        source: AMAZON_SOURCE,
        source_url: buildAmazonIndiaUrl(row.link.asin, associateTag),
      },
      match: {
        id: String(matches[0]._id),
        name: matches[0].name,
        category: row.link.category,
      },
    };
  });

  return {
    source: AMAZON_SOURCE,
    checksum: feedChecksum(associateTag, normalized, rows),
    normalized,
    rows,
    counts: { received: links.length, ...summarize(rows) },
  };
};

module.exports = {
  AMAZON_SOURCE,
  ASIN_PATTERN,
  buildAmazonIndiaUrl,
  normalizeAmazonLink,
  feedChecksum,
  reviewAmazonLinks,
};
