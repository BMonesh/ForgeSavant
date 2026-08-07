import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";
import NotFound from "./NotFound";

describe("NotFound", () => {
  it("explains the missing route and provides recovery destinations", () => {
    render(<MemoryRouter><NotFound /></MemoryRouter>);

    expect(screen.getByRole("heading", { name: "This path is not in the build plan." })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /return home/i })).toHaveAttribute("href", "/");
    expect(screen.getByRole("link", { name: "Open builder" })).toHaveAttribute("href", "/build");
    expect(screen.getByRole("link", { name: "View benchmarks" })).toHaveAttribute("href", "/benchmarks");
  });
});
