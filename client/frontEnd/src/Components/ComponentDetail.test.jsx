import { MemoryRouter, Route, Routes } from "react-router-dom";
import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import ComponentDetail from "./ComponentDetail";
import api from "../services/api";

vi.mock("../services/api", () => ({ default: { get: vi.fn() } }));

const component = {
  _id: "cpu-1",
  name: "AMD Ryzen 5 5600X",
  manufacturer: "AMD",
  price: 14999,
  pricing: { status: "sample", source: "amazon.in", observedAt: null, sourceUrl: "" },
  identity: {
    canonicalKey: "processors:amd-ryzen-5-5600x",
    manufacturerPartNumber: "100-100000065BOX",
    manufacturerPartNumberSourceUrl: "https://www.amd.com/example",
    aliases: [],
    lifecycleStatus: "unknown",
  },
  specifications: { socket: "AM4", cores: 6 },
  priceHistory: [{
    price: 14999,
    source: "amazon.in",
    availability: "unknown",
    recordedAt: "2026-07-21T00:00:00.000Z",
    sourceUrl: "",
    importChecksum: "",
  }],
  retailerMappings: [],
};

describe("ComponentDetail", () => {
  beforeEach(() => {
    api.get.mockReset();
    api.get.mockResolvedValue({ data: { data: component } });
  });

  it("does not present sample catalog prices as retailer observations", async () => {
    render(
      <MemoryRouter initialEntries={["/components/processors/cpu-1"]}>
        <Routes>
          <Route path="/components/:category/:id" element={<ComponentDetail />} />
        </Routes>
      </MemoryRouter>
    );

    expect(await screen.findByRole("heading", { name: component.name })).toBeInTheDocument();
    expect(screen.getByText("Sample catalog value · No retailer observation")).toBeInTheDocument();
    expect(screen.getByText("Sample baseline")).toBeInTheDocument();
    expect(screen.getByText("Not observed")).toBeInTheDocument();
  });
});
