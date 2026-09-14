import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import GamingEstimate from "./GamingEstimate";

const estimate = {
  status: "estimate",
  confidence: "low",
  preset: "High",
  workload: "current demanding games",
  model: { version: "gaming-bands-0.1.0", basis: "Blender Open Data 5.1.1 median score", anchor: "RTX 4060-class score of 3,000 ≈ 60 fps at 1080p High" },
  tiers: [
    { resolution: "1080p", label: "1080p", band: "60-90", bandLabel: "60–90 fps" },
    { resolution: "1440p", label: "1440p", band: "30-60", bandLabel: "30–60 fps" },
    { resolution: "4k", label: "4K", band: "under-30", bandLabel: "Under 30 fps" },
  ],
  assumptions: ["Estimated, not measured. No game was run on this configuration."],
};

describe("GamingEstimate", () => {
  it("shows a band per resolution, labelled as an estimate", () => {
    render(<GamingEstimate gaming={estimate} />);
    expect(screen.getByText("Estimated · not measured")).toBeInTheDocument();
    expect(screen.getByText("60–90 fps")).toBeInTheDocument();
    expect(screen.getByText("Under 30 fps")).toBeInTheDocument();
    expect(screen.getByText(/No game was run/)).toBeInTheDocument();
  });

  it("explains why a card has no estimate instead of hiding it", () => {
    render(<GamingEstimate gaming={{ status: "unavailable", reason: "No estimate for this card: calibrated only for GeForce RTX 40 series.", tiers: [] }} />);
    expect(screen.getByRole("heading", { name: "No estimate for this card" })).toBeInTheDocument();
    expect(screen.getByText(/calibrated only for GeForce RTX 40 series/)).toBeInTheDocument();
    expect(screen.queryByText(/fps/)).not.toBeInTheDocument();
  });

  it("renders nothing before the server has answered", () => {
    const { container } = render(<GamingEstimate gaming={undefined} />);
    expect(container).toBeEmptyDOMElement();
  });
});
