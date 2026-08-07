const crypto = require("node:crypto");

const CATEGORY_ALIASES = {
  processor: "processors",
  processors: "processors",
  cpu: "processors",
  gpu: "gpus",
  gpus: "gpus",
  graphics_card: "gpus",
  graphics_cards: "gpus",
  motherboard: "motherboards",
  motherboards: "motherboards",
  ram: "ram",
  memory: "ram",
  storage: "storage",
  ssd: "storage",
  hdd: "storage",
  power_supply: "powerSupplies",
  power_supplies: "powerSupplies",
  psu: "powerSupplies",
  smps: "powerSupplies",
  cabinet: "cabinets",
  cabinets: "cabinets",
  case: "cabinets",
};

const NOISE_TOKENS = new Set([
  "processor", "graphics", "card", "desktop", "gaming", "memory", "internal",
  "ssd", "hdd", "computer", "component", "with", "for",
]);
const AVAILABILITY = new Set(["in_stock", "out_of_stock", "preorder", "unknown"]);

const cleanText = (value) => String(value ?? "").trim();
const normalizeCategory = (value) => CATEGORY_ALIASES[cleanText(value).toLowerCase().replace(/[\s-]+/g, "_")] || "";
const titleTokens = (value) => new Set(
  cleanText(value)
    .toLowerCase()
    .match(/[a-z0-9]+/g)
    ?.filter((token) => token.length > 1 && !NOISE_TOKENS.has(token)) || []
);
const normalizedTitle = (value) => [...titleTokens(value)].join(" ");

const normalizeOffer = (raw, index, now = new Date()) => {
  const errors = [];
  const name = cleanText(raw?.name);
  const category = normalizeCategory(raw?.category);
  const sourceItemId = cleanText(raw?.source_item_id || raw?.source_sku);
  const price = Number(raw?.price);
  const currency = cleanText(raw?.currency || "INR").toUpperCase();
  const availability = cleanText(raw?.availability || "unknown").toLowerCase().replace(/[\s-]+/g, "_");
  const sourceUrl = cleanText(raw?.source_url);
  const imageUrl = cleanText(raw?.image_url);
  const manufacturerPartNumber = cleanText(raw?.manufacturer_part_number || raw?.mpn).toUpperCase();
  const observedAt = cleanText(raw?.observed_at || raw?.collected_at);
  const observedDate = new Date(observedAt);

  if (!name) errors.push("name is required");
  if (!category) errors.push("category is not supported");
  if (!sourceItemId) errors.push("source_item_id is required");
  if (!Number.isFinite(price) || price <= 0) errors.push("price must be a positive number");
  if (currency !== "INR") errors.push("currency must be INR");
  if (!AVAILABILITY.has(availability)) errors.push("availability is not supported");
  if (!/^https:\/\//i.test(sourceUrl)) errors.push("source_url must be an HTTPS URL");
  if (imageUrl && !/^https:\/\//i.test(imageUrl)) errors.push("image_url must be an HTTPS URL");
  if (!observedAt || Number.isNaN(observedDate.getTime())) errors.push("observed_at must be an ISO timestamp");
  if (!Number.isNaN(observedDate.getTime()) && observedDate.getTime() > now.getTime() + 15 * 60 * 1000) {
    errors.push("observed_at cannot be in the future");
  }

  return {
    index,
    errors,
    offer: {
      name,
      category,
      source_item_id: sourceItemId,
      price,
      currency,
      availability,
      source_url: sourceUrl,
      image_url: imageUrl,
      manufacturer_part_number: manufacturerPartNumber,
      observed_at: Number.isNaN(observedDate.getTime()) ? observedAt : observedDate.toISOString(),
    },
  };
};

const scoreTitleMatch = (expectedName, candidateName) => {
  const expected = titleTokens(expectedName);
  const candidate = titleTokens(candidateName);
  if (!expected.size || !candidate.size) return 0;
  if (normalizedTitle(expectedName) === normalizedTitle(candidateName)) return 1;
  const overlap = [...expected].filter((token) => candidate.has(token)).length;
  return overlap / expected.size;
};

const matchOffer = (offer, catalogItems) => {
  if (offer.manufacturer_part_number) {
    const partNumberMatches = catalogItems.filter((item) => cleanText(item.identity?.manufacturerPartNumber).toUpperCase() === offer.manufacturer_part_number);
    if (partNumberMatches.length === 1) return { status: "accepted", match: partNumberMatches[0], score: 1, matchSignal: "manufacturer_part_number" };
    if (partNumberMatches.length > 1) return {
      status: "ambiguous",
      reason: "Manufacturer part number is assigned to multiple catalog records",
      candidates: partNumberMatches.map((item) => ({ id: String(item._id), name: item.name, score: 1 })),
    };
  }
  const rankedAll = catalogItems
    .map((item) => ({ item, score: scoreTitleMatch(offer.name, item.name) }))
    .sort((a, b) => b.score - a.score || String(a.item.name).localeCompare(String(b.item.name)));
  const candidatePreview = rankedAll
    .filter(({ score }) => score > 0)
    .slice(0, 5)
    .map(({ item, score }) => ({ id: String(item._id), name: item.name, score }));
  const ranked = rankedAll.filter(({ score }) => score >= 0.8);

  if (!ranked.length) return { status: "unmatched", reason: "No catalog model reached the 80% token threshold", candidates: candidatePreview };
  if (ranked[1] && Math.abs(ranked[0].score - ranked[1].score) < 0.02) {
    return {
      status: "ambiguous",
      reason: "Multiple catalog models scored equally",
      candidates: ranked.slice(0, 3).map(({ item, score }) => ({ id: String(item._id), name: item.name, score })),
    };
  }
  return { status: "accepted", match: ranked[0].item, score: ranked[0].score };
};

const canonicalFeed = (source, normalizedOffers, resolutions = []) => JSON.stringify({
  source: cleanText(source).toLowerCase(),
  offers: normalizedOffers.map(({ offer }) => offer),
  resolutions: [...resolutions]
    .map((resolution) => ({ index: Number(resolution.index), componentId: cleanText(resolution.componentId) }))
    .sort((left, right) => left.index - right.index),
});
const feedChecksum = (source, normalizedOffers, resolutions = []) => crypto
  .createHash("sha256")
  .update(canonicalFeed(source, normalizedOffers, resolutions))
  .digest("hex");

const summarizeReview = (rows) => rows.reduce((counts, row) => {
  counts[row.status] += 1;
  return counts;
}, { accepted: 0, ambiguous: 0, unmatched: 0, rejected: 0 });

const reviewOffers = async ({ source, offers, catalogModels, now = new Date(), mappings = [], resolutions = [] }) => {
  const sourceName = cleanText(source).toLowerCase();
  if (!/^[a-z0-9][a-z0-9._-]{1,63}$/.test(sourceName)) {
    const error = new Error("source must be 2-64 lowercase letters, numbers, dots, dashes, or underscores");
    error.statusCode = 400;
    throw error;
  }
  if (!Array.isArray(offers) || offers.length < 1 || offers.length > 500) {
    const error = new Error("offers must contain between 1 and 500 records");
    error.statusCode = 400;
    throw error;
  }

  const normalized = offers.map((offer, index) => normalizeOffer(offer, index, now));
  const categories = [...new Set(normalized.filter((row) => !row.errors.length).map((row) => row.offer.category))];
  const catalog = Object.fromEntries(await Promise.all(categories.map(async (category) => [
    category,
    await catalogModels[category].find().lean(),
  ])));
  const mappingByItem = new Map(mappings.filter((mapping) => mapping.active !== false).map((mapping) => [mapping.sourceItemId, mapping]));
  const resolutionByIndex = new Map(resolutions.map((resolution) => [Number(resolution.index), cleanText(resolution.componentId)]));

  const rows = normalized.map((row) => {
    if (row.errors.length) return { index: row.index, status: "rejected", errors: row.errors, offer: row.offer };
    const categoryItems = catalog[row.offer.category] || [];
    const savedMapping = mappingByItem.get(row.offer.source_item_id);
    if (savedMapping) {
      const mappedItem = categoryItems.find((item) => String(item._id) === String(savedMapping.componentId));
      if (mappedItem) {
        return {
          index: row.index,
          status: "accepted",
          score: savedMapping.confidence,
          matchMethod: savedMapping.matchMethod || "automatic",
          matchedBy: "saved_mapping",
          offer: row.offer,
          match: { id: String(mappedItem._id), name: mappedItem.name, category: row.offer.category, currentPrice: mappedItem.price },
        };
      }
    }
    const resolution = resolutionByIndex.get(row.index);
    if (resolution) {
      const resolvedItem = categoryItems.find((item) => String(item._id) === resolution);
      if (!resolvedItem) return { index: row.index, status: "rejected", errors: ["manual resolution is not a valid component in this category"], offer: row.offer };
      return {
        index: row.index,
        status: "accepted",
        score: 1,
        matchMethod: "manual",
        offer: row.offer,
        match: { id: String(resolvedItem._id), name: resolvedItem.name, category: row.offer.category, currentPrice: resolvedItem.price },
      };
    }
    const result = matchOffer(row.offer, categoryItems);
    if (result.status !== "accepted") return { index: row.index, offer: row.offer, ...result };
    return {
      index: row.index,
      status: "accepted",
      score: result.score,
      matchMethod: "automatic",
      matchedBy: result.matchSignal || "title_tokens",
      offer: row.offer,
      match: {
        id: String(result.match._id),
        name: result.match.name,
        category: row.offer.category,
        currentPrice: result.match.price,
      },
    };
  });

  return {
    source: sourceName,
    checksum: feedChecksum(sourceName, normalized, resolutions),
    rows,
    counts: { received: offers.length, ...summarizeReview(rows) },
    normalized,
  };
};

module.exports = {
  normalizeCategory,
  normalizeOffer,
  titleTokens,
  scoreTitleMatch,
  matchOffer,
  feedChecksum,
  reviewOffers,
};
