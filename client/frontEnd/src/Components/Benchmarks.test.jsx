import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import Benchmarks from "./Benchmarks";
import api from "../services/api";

vi.mock("../services/api", () => ({ default: { get: vi.fn() } }));

const summary = {
  generatedAt: "2026-08-07T00:00:00Z",
  counts: { total: 2, processors: 1, gpus: 1 },
  caveats: ["Compare only within a category."],
  records: [
    { category: "gpus", catalogName: "Example GPU", manufacturerPartNumber: "GPU-1", metricValue: 2400, unit: "Blender Benchmark points", sampleCount: 10, observedAt: "2026-08-07T00:00:00Z", sourceRecordUrl: "https://example.test/gpu", categoryRank: 1 },
    { category: "processors", catalogName: "Example CPU", manufacturerPartNumber: "CPU-1", metricValue: 400, unit: "Blender Benchmark points", sampleCount: 20, observedAt: "2026-08-07T00:00:00Z", sourceRecordUrl: "https://example.test/cpu", categoryRank: 1 },
  ],
};

describe("Benchmarks", () => {
  beforeEach(() => api.get.mockReset());

  it("keeps CPU and GPU rankings separate and links to evidence", async () => {
    const user = userEvent.setup();
    api.get.mockResolvedValue({ data: { data: summary } });
    render(<Benchmarks />);

    expect(await screen.findByRole("heading", { name: "Compare one workload. See the limits." })).toBeInTheDocument();
    expect(screen.getByText("Example GPU")).toBeInTheDocument();
    expect(screen.queryByText("Example CPU")).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Open source/ })).toHaveAttribute("href", "https://example.test/gpu");

    await user.click(screen.getByRole("button", { name: /Processors/ }));
    expect(screen.getByText("Example CPU")).toBeInTheDocument();
    expect(screen.queryByText("Example GPU")).not.toBeInTheDocument();
  });
});
