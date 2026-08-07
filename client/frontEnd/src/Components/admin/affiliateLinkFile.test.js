import { describe, expect, it } from "vitest";
import { parseAffiliateLinkFile } from "./affiliateLinkFile";

describe("Amazon affiliate-link file parser", () => {
  it("accepts an object or array feed", () => {
    expect(parseAffiliateLinkFile('{"links":[{"asin":"B09V2W32QX"}]}')).toHaveLength(1);
    expect(parseAffiliateLinkFile('[{"asin":"B09V2W32QX"}]')).toHaveLength(1);
  });

  it("rejects malformed or empty feeds", () => {
    expect(() => parseAffiliateLinkFile("{")).toThrow(/valid JSON/i);
    expect(() => parseAffiliateLinkFile('{"links":[]}')).toThrow(/non-empty links array/i);
  });
});
