const GENERIC_TOKENS = new Set([
  "processor", "graphics", "card", "desktop", "gaming", "memory", "internal",
  "ssd", "hdd", "computer", "component", "with", "for",
]);

const identityTokens = (value) => String(value || "")
  .toLowerCase()
  .match(/[a-z0-9]+/g)
  ?.filter((token) => token.length > 1 && !GENERIC_TOKENS.has(token)) || [];

const normalizedIdentityName = (value) => identityTokens(value).join(" ");
const canonicalKey = (category, name) => `${category}:${normalizedIdentityName(name).replace(/\s+/g, "-")}`;

const uniqueAliases = (...aliasGroups) => [...new Set(
  aliasGroups.flat().map((alias) => String(alias || "").trim()).filter(Boolean)
)];

const initialPriceHistory = (component) => ({
  price: component.price,
  currency: component.provenance?.currency || "INR",
  availability: component.provenance?.availability || "unknown",
  source: component.provenance?.source || "catalog",
  sourceUrl: component.provenance?.source_url || "",
  sourceItemId: component.provenance?.source_item_id || "",
  observedAt: component.provenance?.collected_at || null,
  importChecksum: component.provenance?.import_checksum || "",
  recordedAt: new Date(),
});

const chooseDuplicateKeeper = (components) => [...components].sort((a, b) => {
  const liveDifference = Number(b.provenance?.data_status === "live") - Number(a.provenance?.data_status === "live");
  if (liveDifference) return liveDifference;
  const urlDifference = Number(Boolean(b.provenance?.source_url)) - Number(Boolean(a.provenance?.source_url));
  if (urlDifference) return urlDifference;
  const priceDifference = Number(a.price || Infinity) - Number(b.price || Infinity);
  if (priceDifference) return priceDifference;
  return String(a.name).localeCompare(String(b.name));
})[0];

const specificationsEqual = (left, right) => JSON.stringify(left || {}) === JSON.stringify(right || {});

module.exports = {
  identityTokens,
  normalizedIdentityName,
  canonicalKey,
  uniqueAliases,
  initialPriceHistory,
  chooseDuplicateKeeper,
  specificationsEqual,
};
