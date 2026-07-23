import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import ComponentPicker from "./ComponentPicker";

const items = [
  { _id: "1", name: "Ryzen 7 7800X3D", manufacturer: "AMD", price: 36000, pricing: { status: "live", source: "flipkart_affiliate", observedAt: "2026-07-20T12:00:00.000Z" }, specifications: { socket: "AM5", cores: 8, threads: 16, tdp: "120W" } },
  { _id: "2", name: "Core i7 14700K", manufacturer: "Intel", price: 38000, specifications: { socket: "LGA 1700", cores: 20, threads: 28, tdp: "125W" } },
];

const renderPicker = (onSelect = vi.fn()) => {
  render(<ComponentPicker stepId="processor" title="CPU" description="Choose a processor." items={items} selectedId="" loading={false} error="" onRetry={vi.fn()} onSelect={onSelect} canContinue={false} rowImage="fallback.png" />);
  return onSelect;
};

describe("ComponentPicker", () => {
  it("searches component records and selects a result", async () => {
    const user = userEvent.setup();
    const onSelect = renderPicker();

    await user.type(screen.getByPlaceholderText("Search components"), "Ryzen");
    expect(screen.getByRole("option", { name: /Ryzen 7/i })).toBeInTheDocument();
    expect(screen.getByText(/Live price · flipkart affiliate/i)).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: /Core i7/i })).not.toBeInTheDocument();
    await user.click(screen.getByRole("option", { name: /Ryzen 7/i }));
    expect(onSelect).toHaveBeenCalledWith(items[0]);
  });

  it("shows an actionable empty state when filters have no matches", async () => {
    const user = userEvent.setup();
    renderPicker();
    await user.type(screen.getByPlaceholderText("Search components"), "Threadripper");
    expect(screen.getByText("No matching parts")).toBeInTheDocument();
    expect(screen.getByText("0 options")).toBeInTheDocument();
  });
});
