import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import AdminDataQuality from "./AdminDataQuality";
import api from "../services/api";

vi.mock("../services/api", () => ({
  default: { get: vi.fn() },
}));

const summary = {
  generatedAt: "2026-07-22T07:05:26.203Z",
  grain: "one immutable normalized source observation per source product content version",
  status: "healthy",
  freshness: { ageHours: 2.4, thresholdHours: 168 },
  catalog: {
    verifiedProducts: 58,
    observedProducts: 14,
    coverageRate: 14 / 58,
    openIcecatAvailable: 14,
    openIcecatRestricted: 31,
    openIcecatUnavailable: 13,
  },
  pipeline: { runs: 1, received: 14, accepted: 14, duplicates: 0, quarantined: 0, validationPassRate: 1 },
  retail: { priceObservations: 8, productsWithPriceHistory: 4, currentOffers: 4, productsWithCurrentOffers: 4, retailers: 1 },
  retailSnapshot: { scannedComponents: 58, scannedPriceHistoryEntries: 59, eligibleOffers: 0, skippedEntries: 59 },
  coverageQueue: { verified: 58, covered: 14, manufacturerReady: 44, sourceMissing: 0 },
  outcomes: { observations: 3, pseudonymousSubjects: 2, builds: 3 },
  benchmarks: { observations: 6, currentObservations: 3, products: 3, metrics: 2, sources: 1, observationDates: 2 },
  quality: { identityCompletenessRate: 1, gtinCoverageRate: 13 / 14, imageCoverageRate: 1, quarantineRate: 0 },
  categories: {
    gpus: { sourceCoverage: 6, verifiedCatalogProducts: 11 },
    storage: { sourceCoverage: 2, verifiedCatalogProducts: 4 },
  },
  caveats: ["Open Icecat is not a retailer price source."],
  definitions: { catalogCoverage: "Observed products divided by verified products." },
  modelReadiness: { uses: [
    { use: "Descriptive data-quality monitoring", status: "ready", reason: "Validated evidence exists." },
    { use: "India price prediction or forecasting", status: "blocked", reason: "No price history exists." },
  ] },
};

describe("AdminDataQuality", () => {
  beforeEach(() => api.get.mockReset());

  it("renders source-backed catalog and pipeline measures", async () => {
    api.get.mockResolvedValue({ data: { data: summary } });

    render(<AdminDataQuality />);

    expect(await screen.findByRole("heading", { name: "Know what the catalog can prove." })).toBeInTheDocument();
    expect(screen.getByText("24.1%")).toBeInTheDocument();
    expect(screen.getByText("92.9%")).toBeInTheDocument();
    expect(screen.getByText("Graphics cards")).toBeInTheDocument();
    expect(screen.getByText("Open Icecat is not a retailer price source.")).toBeInTheDocument();
    expect(screen.getByText("Model readiness")).toBeInTheDocument();
    expect(screen.getByText("Evidence work queue")).toBeInTheDocument();
    expect(screen.getByText("Manufacturer-ready gaps")).toBeInTheDocument();
    expect(screen.getByText("Planning price rows excluded")).toBeInTheDocument();
    expect(screen.getByText("Benchmarked products")).toBeInTheDocument();
    expect(screen.getByText("Benchmark snapshots")).toBeInTheDocument();
    expect(screen.getByText("Current benchmark records")).toBeInTheDocument();
    expect(screen.getByText("India price prediction or forecasting")).toBeInTheDocument();
    expect(api.get).toHaveBeenCalledWith("/api/v1/admin/analytics/data-quality");
  });

  it("shows the API error and retries", async () => {
    const user = userEvent.setup();
    api.get
      .mockRejectedValueOnce({ response: { data: { error: "Analytics summary is unavailable" } } })
      .mockResolvedValueOnce({ data: { data: summary } });

    render(<AdminDataQuality />);

    expect(await screen.findByText("Analytics summary is unavailable")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Try again" }));
    expect(await screen.findByText("Pipeline healthy")).toBeInTheDocument();
    expect(api.get).toHaveBeenCalledTimes(2);
  });
});
