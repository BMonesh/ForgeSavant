const DEFAULT_FRESHNESS_HOURS = 24;

const classifyPricing = (provenance = {}, now = new Date(), freshnessHours = DEFAULT_FRESHNESS_HOURS) => {
  if (provenance.data_status !== "live") return "sample";
  const observedAt = new Date(provenance.collected_at);
  if (Number.isNaN(observedAt.getTime())) return "stale";
  const ageMs = now.getTime() - observedAt.getTime();
  return ageMs >= 0 && ageMs <= freshnessHours * 60 * 60 * 1000 ? "live" : "stale";
};

const presentCatalogItem = (item, now = new Date(), freshnessHours = DEFAULT_FRESHNESS_HOURS) => {
  const { imported_by: _operatorEmail, ...publicProvenance } = item.provenance || {};
  const productContentEvidence = (item.productContentEvidence || []).map((entry) => {
    const { importedBy: _importedBy, importChecksum: _importChecksum, ...publicEntry } = entry;
    return publicEntry;
  });
  return {
    ...item,
    provenance: publicProvenance,
    productContentEvidence,
    pricing: {
      status: classifyPricing(item.provenance, now, freshnessHours),
      source: item.provenance?.source || "catalog",
      sourceUrl: item.provenance?.source_url || "",
      observedAt: item.provenance?.collected_at || null,
      availability: item.provenance?.availability || "unknown",
      currency: item.provenance?.currency || "INR",
    },
  };
};

const summarizePricing = (items) => items.reduce(
  (summary, item) => {
    const status = item.pricing?.status || "sample";
    summary[status] = (summary[status] || 0) + 1;
    return summary;
  },
  { live: 0, stale: 0, sample: 0 }
);

module.exports = { classifyPricing, presentCatalogItem, summarizePricing, DEFAULT_FRESHNESS_HOURS };
