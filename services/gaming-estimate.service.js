/**
 * Estimated gaming frame-rate bands for a graphics card.
 *
 * This is a projection, not a measurement, and everything it returns says so.
 * No measured frame rates are available under terms that allow showing them, so
 * the estimate is derived from the one licensed performance signal the catalog
 * has: Blender Open Data medians matched to the exact part.
 *
 * Why it only covers GeForce RTX 40 series
 * ----------------------------------------
 * Blender's GPU benchmark ranks cards correctly for gaming only within a single
 * architecture. In this catalog the Radeon RX 7800 XT scores 2,872, below the
 * RTX 4060's 3,063, although in games it sits nearer an RTX 4070; the RTX 3060
 * scores 1,989, some 35% under the 4060, a far wider gap than games show,
 * because Ada's newer ray-tracing hardware accelerates Blender specifically.
 * Applying one scale to all of them would tell someone an RX 7600 cannot hold
 * 30 fps at 1080p. Cards outside the calibrated generation get a stated reason
 * instead of a wrong band.
 *
 * Method
 * ------
 * - Anchor: an RTX 4060-class score of 3,000 is taken as about 60 fps at 1080p
 *   on a High preset in current demanding titles. This is a judgement, recorded
 *   in the model version, and deliberately at the conservative end of commonly
 *   reported results.
 * - Frame rate scales linearly with score within the generation.
 * - Resolution cost grows with pixel count raised to 0.75, because frame rate
 *   falls more slowly than pixel count rises as other stages stop scaling.
 * - Results are reported as bands, never as a number, so the output cannot be
 *   read as more precise than the method is.
 * - The processor is not modelled. At 1080p, especially high refresh rates, it
 *   is often what limits frame rate.
 */

const GAMING_MODEL_VERSION = "gaming-bands-0.1.0";
const ANCHOR = { score: 3000, fps: 60 };
const PIXEL_EXPONENT = 0.75;
const BASE_PIXELS = 1920 * 1080;

const RESOLUTIONS = [
  { key: "1080p", label: "1080p", pixels: 1920 * 1080 },
  { key: "1440p", label: "1440p", pixels: 2560 * 1440 },
  { key: "4k", label: "4K", pixels: 3840 * 2160 },
];

const BANDS = [
  { min: 144, key: "144-plus", label: "144+ fps" },
  { min: 90, key: "90-144", label: "90–144 fps" },
  { min: 60, key: "60-90", label: "60–90 fps" },
  { min: 30, key: "30-60", label: "30–60 fps" },
  { min: 0, key: "under-30", label: "Under 30 fps" },
];

const CALIBRATED_GENERATION = /\bRTX\s*40\d0\b/i;

const chipVendor = (name) => {
  const text = String(name || "");
  if (/\b(GeForce|RTX|GTX)\b/i.test(text)) return "nvidia";
  if (/\b(Radeon|RX\s*\d{3,4})\b/i.test(text)) return "amd";
  if (/\bArc\b/i.test(text)) return "intel";
  return "unknown";
};

/** Projected frame rate before banding. Exported for tests, never returned by the API. */
const projectedFrameRate = (score, pixels) =>
  ANCHOR.fps * (Number(score) / ANCHOR.score) / (pixels / BASE_PIXELS) ** PIXEL_EXPONENT;

const bandFor = (fps) => BANDS.find((band) => fps >= band.min);

const unavailable = (reason) => ({
  status: "unavailable",
  model: { version: GAMING_MODEL_VERSION },
  reason,
  tiers: [],
});

const estimateGamingTiers = (gpu, benchmarkRecord) => {
  if (!gpu) return unavailable("Choose a graphics card to see an estimate.");

  const vendor = chipVendor(gpu.name);
  if (!CALIBRATED_GENERATION.test(gpu.name || "")) {
    const why = vendor === "amd"
      ? "Blender's benchmark understates Radeon gaming performance, ranking an RX 7800 XT below an RTX 4060"
      : vendor === "intel"
        ? "Blender's benchmark does not track Arc gaming performance"
        : "Blender's benchmark favours newer GeForce hardware, so an earlier generation would be under-rated";
    return unavailable(`No estimate for this card: the model is calibrated only for GeForce RTX 40 series, and ${why}. Showing no band is more accurate than showing a wrong one.`);
  }

  const score = Number(benchmarkRecord?.metricValue);
  if (!Number.isFinite(score) || score <= 0) {
    return unavailable("No public benchmark matches this exact part number, so there is nothing to base an estimate on.");
  }

  return {
    status: "estimate",
    confidence: "low",
    model: {
      version: GAMING_MODEL_VERSION,
      basis: `${benchmarkRecord.benchmarkName || "Blender Open Data"} median score`,
      anchor: "RTX 4060-class score of 3,000 ≈ 60 fps at 1080p High",
    },
    preset: "High",
    workload: "current demanding games",
    tiers: RESOLUTIONS.map((resolution) => {
      const band = bandFor(projectedFrameRate(score, resolution.pixels));
      return { resolution: resolution.key, label: resolution.label, band: band.key, bandLabel: band.label };
    }),
    assumptions: [
      "Estimated, not measured. No game was run on this configuration.",
      "Derived from a rendering benchmark calibrated to one GPU generation, not from game results.",
      "The processor is not modelled; at 1080p it often limits frame rate.",
      "Real results vary widely by game, driver, settings, upscaling and cooling.",
    ],
  };
};

module.exports = {
  GAMING_MODEL_VERSION,
  estimateGamingTiers,
  projectedFrameRate,
  chipVendor,
};
