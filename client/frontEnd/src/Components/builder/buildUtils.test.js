import { describe, expect, it } from "vitest";
import {
  calculateRecommendedPsu,
  emptySelection,
  getBuildTotal,
  getItemsForStep,
  getFirstIncompleteStep,
  hydrateSavedBuild,
  inferSupportedRamTypes,
  isBuildComplete,
  resetAfterStep,
} from "./buildUtils";

const part = (name, specifications = {}, price = 100) => ({
  _id: name,
  name,
  manufacturer: "Maker",
  specifications,
  price,
});

describe("builder compatibility rules", () => {
  it("filters motherboards by the selected CPU socket", () => {
    const selection = { ...emptySelection, processor: part("CPU", { socket: "AM5" }) };
    const catalog = {
      motherboard: [part("AM5 board", { socket: "AM5" }), part("AM4 board", { socket: "AM4" })],
    };

    expect(getItemsForStep("motherboard", selection, catalog).map((item) => item.name)).toEqual(["AM5 board"]);
  });

  it("requires a PSU tier above calculated draw and headroom", () => {
    const cpu = part("CPU", { tdp: "120W" });
    const gpu = part("GPU", { tdp: "285W" });
    expect(calculateRecommendedPsu(cpu, gpu)).toBe(650);
  });

  it("filters PSU and RAM choices from upstream selections", () => {
    const selection = {
      ...emptySelection,
      processor: part("CPU", { tdp: "120W" }),
      gpu: part("GPU", { tdp: "285W" }),
      motherboard: part("Board", { socket: "AM5" }),
    };
    const catalog = {
      smps: [part("550W", { wattage: "550W" }), part("750W", { wattage: "750W" })],
      ram: [part("DDR4 kit", { type: "DDR4" }), part("DDR5 kit", { type: "DDR5" })],
    };

    expect(inferSupportedRamTypes(selection.motherboard)).toEqual(["DDR5"]);
    expect(getItemsForStep("smps", selection, catalog).map((item) => item.name)).toEqual(["750W"]);
    expect(getItemsForStep("ram", selection, catalog).map((item) => item.name)).toEqual(["DDR5 kit"]);
  });

  it("uses an explicit motherboard memory type before socket inference", () => {
    const board = part("LGA board", { socket: "LGA 1700", memory_type: "DDR4" });
    expect(inferSupportedRamTypes(board)).toEqual(["DDR4"]);
  });

  it("clears downstream choices when an earlier selection changes", () => {
    const selection = {
      ...emptySelection,
      platform: "AMD",
      processor: part("CPU"),
      motherboard: part("Board"),
      gpu: part("GPU"),
    };

    const reset = resetAfterStep(selection, "processor");
    expect(reset.processor).toBe(selection.processor);
    expect(reset.motherboard).toBeNull();
    expect(reset.gpu).toBeNull();
  });

  it("calculates totals and only completes fully populated builds", () => {
    const complete = Object.fromEntries(
      Object.keys(emptySelection).map((key) => [key, key === "platform" ? "AMD" : part(key, {}, 100)])
    );
    expect(isBuildComplete(complete)).toBe(true);
    expect(getBuildTotal(complete)).toBe(800);
    expect(isBuildComplete({ ...complete, cabinet: null })).toBe(false);
  });

  it("rehydrates a saved record from the current catalog", () => {
    const catalog = {
      cpu: [{ ...part("Saved CPU"), manufacturer: "AMD" }],
      motherboard: [part("Saved Board")],
      gpu: [part("Saved GPU")],
      storage: [part("Primary"), part("Secondary")],
      ram: [part("Saved RAM")],
      smps: [part("Saved PSU")],
      cabinet: [part("Saved Case")],
    };
    const hydrated = hydrateSavedBuild({
      cpu: "Saved CPU",
      motherboard: "Saved Board",
      gpu: "Saved GPU",
      primaryStorage: "Primary",
      secondaryStorage: "Secondary",
      ram: "Saved RAM",
      powerSupply: "Saved PSU",
      cabinet: "Saved Case",
    }, catalog);

    expect(hydrated.platform).toBe("AMD");
    expect(hydrated.cabinet.name).toBe("Saved Case");
    expect(getFirstIncompleteStep(hydrated)).toBe("review");
  });

  it("rehydrates renamed products by stable component id", () => {
    const renamedCpu = { ...part("Verified CPU"), _id: "cpu-1", manufacturer: "AMD" };
    const hydrated = hydrateSavedBuild({ cpu: "Legacy CPU", componentIds: { processor: "cpu-1" } }, {
      cpu: [renamedCpu], motherboard: [], gpu: [], storage: [], ram: [], smps: [], cabinet: [],
    });
    expect(hydrated.processor).toBe(renamedCpu);
  });
});
