import { describe, expect, it } from "vitest";
import { parseCsv, parseOfferBundleText, parseOfferText } from "./offerFile";

describe("offer file parser", () => {
  it("parses quoted CSV fields and normalized headers", () => {
    const rows = parseCsv('Name,Source SKU,Price\r\n"Ryzen 5, 5600X",SKU-1,12499');
    expect(rows).toEqual([{ name: "Ryzen 5, 5600X", source_sku: "SKU-1", price: "12499" }]);
  });

  it("accepts JSON arrays and object envelopes", () => {
    expect(parseOfferText('[{"name":"CPU"}]', "feed.json")).toEqual([{ name: "CPU" }]);
    expect(parseOfferText('{"offers":[{"name":"GPU"}]}', "feed.json")).toEqual([{ name: "GPU" }]);
  });

  it("carries an exported source identifier into the admin workflow", () => {
    expect(parseOfferBundleText('{"source":"Flipkart_Affiliate","offers":[{"name":"GPU"}]}', "feed.json"))
      .toEqual({ source: "flipkart_affiliate", offers: [{ name: "GPU" }] });
  });

  it("rejects malformed files", () => {
    expect(() => parseOfferText("name\n", "feed.csv")).toThrow(/header and at least one offer/i);
    expect(() => parseOfferText("name,price\nCPU", "feed.csv")).toThrow(/expected 2/i);
    expect(() => parseOfferText('{"items":[]}', "feed.json")).toThrow(/offers array/i);
  });
});
