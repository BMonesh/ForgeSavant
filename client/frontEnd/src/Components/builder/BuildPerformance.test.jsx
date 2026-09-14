import { MemoryRouter } from "react-router-dom";
import { render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import BuildPerformance from "./BuildPerformance";
import api from "../../services/api";

vi.mock("../../services/api", () => ({ default: { get: vi.fn() } }));

const record = (category, catalogName, manufacturerPartNumber, metricValue) => ({
  category,
  catalogName,
  manufacturerPartNumber,
  metricValue,
  sampleCount: 40,
  unit: "Blender Benchmark points",
  benchmarkName: "Blender Open Data 5.1.1",
  sourceRecordUrl: "https://opendata.blender.org/benchmarks/query/",
});

const benchmarks = {
  caveats: ["Scores are comparable only within the same benchmark version and component category."],
  records: [
    record("processors", "Fast CPU", "CPU-FAST", 500),
    record("processors", "Mid CPU", "CPU-MID", 250),
    record("gpus", "Fast GPU", "GPU-FAST", 10000),
    record("gpus", "Slow GPU", "GPU-SLOW", 2500),
  ],
};

const part = (name, manufacturerPartNumber) => ({ name, identity: { manufacturerPartNumber } });

const renderPanel = (selection) => render(
  <MemoryRouter>
    <BuildPerformance selection={selection} />
  </MemoryRouter>
);

describe("BuildPerformance", () => {
  // Braced on purpose: a concise arrow would return the mock, and Vitest calls a
  // function returned from beforeEach as teardown, invoking api.get after each test.
  beforeEach(() => {
    api.get.mockReset();
  });

  it("ranks the selected parts against the catalog by exact part number", async () => {
    api.get.mockResolvedValue({ data: { data: benchmarks } });
    renderPanel({ processor: part("Mid CPU", "cpu-mid"), gpu: part("Slow GPU", "GPU-SLOW") });

    const cpu = await screen.findByRole("article", { name: "Processor benchmark ranking" });
    expect(within(cpu).getByText("#2 of 2")).toBeInTheDocument();
    expect(within(cpu).getByText("50%")).toBeInTheDocument();
    expect(within(cpu).getByRole("listitem", { current: true })).toHaveTextContent("Mid CPU");

    const gpu = screen.getByRole("article", { name: "Graphics card benchmark ranking" });
    expect(within(gpu).getByText("#2 of 2")).toBeInTheDocument();
    expect(within(gpu).getByText("25%")).toBeInTheDocument();
  });

  it("does not borrow a score when no benchmark matches the part number", async () => {
    api.get.mockResolvedValue({ data: { data: benchmarks } });
    renderPanel({ processor: part("Unbenchmarked CPU", "CPU-NONE"), gpu: part("Fast GPU", "GPU-FAST") });

    const cpu = await screen.findByRole("article", { name: "Processor benchmark ranking" });
    expect(within(cpu).getByText(/No public benchmark matches this exact part number/)).toBeInTheDocument();
    expect(within(cpu).queryByRole("listitem", { current: true })).not.toBeInTheDocument();
  });

  it("warns that CPU and GPU scores are on different scales", async () => {
    api.get.mockResolvedValue({ data: { data: benchmarks } });
    renderPanel({ processor: part("Fast CPU", "CPU-FAST"), gpu: part("Fast GPU", "GPU-FAST") });
    expect(await screen.findByText(/different scales/)).toBeInTheDocument();
  });

  it("reports unavailable evidence instead of failing when the response is unusable", async () => {
    api.get.mockResolvedValue({ data: { data: { processors: [] } } });
    renderPanel({ processor: part("Fast CPU", "CPU-FAST"), gpu: part("Fast GPU", "GPU-FAST") });
    expect(await screen.findByText(/Benchmark evidence is unavailable/)).toBeInTheDocument();
  });

  it("reports unavailable evidence when the request fails", async () => {
    api.get.mockRejectedValue({ response: { status: 503, data: { error: "Benchmark summary is unavailable" } } });
    renderPanel({ processor: part("Fast CPU", "CPU-FAST"), gpu: part("Fast GPU", "GPU-FAST") });
    expect(await screen.findByText(/Benchmark evidence is unavailable/)).toBeInTheDocument();
  });
});
