import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";
import Partners from "./Partners";

describe("Partners", () => {
  it("publishes the retailer feed request and evidence boundaries", () => {
    render(<MemoryRouter><Partners /></MemoryRouter>);

    expect(screen.getByRole("heading", { name: /Help builders compare current PC-part offers/i })).toBeInTheDocument();
    expect(screen.getByText("No customer or order data is requested.")).toBeInTheDocument();
    expect(screen.getByText("Affiliate links cannot become price observations.")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Discuss a data feed/i })).toHaveAttribute("href", expect.stringContaining("mailto:2005.monesh@gmail.com"));
    expect(screen.getByRole("link", { name: /Download CSV template/i })).toHaveAttribute("href", "/offer-feed-template.csv");
  });
});
