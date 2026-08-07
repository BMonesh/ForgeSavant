import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";
import About from "./About";

describe("About", () => {
  it("explains evidence boundaries and links to inspectable workflows", () => {
    render(<MemoryRouter><About /></MemoryRouter>);
    expect(screen.getByRole("heading", { name: "A PC plan should be inspectable." })).toBeInTheDocument();
    expect(screen.getByText("Retail offers")).toBeInTheDocument();
    expect(screen.getByText(/Paid links remain destinations/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Open the builder/ })).toHaveAttribute("href", "/build");
    expect(screen.getByRole("link", { name: /affiliate disclosure/ })).toHaveAttribute("href", "/affiliate-disclosure");
  });
});
