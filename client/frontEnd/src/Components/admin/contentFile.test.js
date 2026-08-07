import { describe, expect, it } from "vitest";
import { parseContentFile } from "./contentFile";

describe("parseContentFile", () => {
  it("accepts the exported Open Icecat contract", async () => {
    const payload = { schema_version: "1.0", source: "open_icecat", observations: [{ observation_id: "obs-1" }] };
    const file = new File([JSON.stringify(payload)], "content.json", { type: "application/json" });
    await expect(parseContentFile(file)).resolves.toEqual(payload);
  });

  it("accepts the combined product-content export contract", async () => {
    const payload = { schema_version: "1.0", source: "forgesavant_product_content", observations: [{ observation_id: "obs-1" }] };
    const file = new File([JSON.stringify(payload)], "content.json", { type: "application/json" });
    await expect(parseContentFile(file)).resolves.toEqual(payload);
  });

  it("rejects unrelated JSON", async () => {
    const file = new File([JSON.stringify({ observations: [] })], "content.json", { type: "application/json" });
    await expect(parseContentFile(file)).rejects.toThrow(/contract/i);
  });
});
