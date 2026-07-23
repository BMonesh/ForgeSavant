import { beforeEach, describe, expect, it } from "vitest";
import { clearBuildDraft, loadBuildDraft, saveBuildDraft } from "./buildDraft";

describe("build draft persistence", () => {
  beforeEach(() => localStorage.clear());

  it("survives route changes and can be cleared after save", () => {
    const draft = {
      currentStepId: "gpu",
      sourceSaveId: null,
      selection: { platform: "AMD", processor: { _id: "cpu-1", name: "Ryzen" } },
    };
    saveBuildDraft(draft);
    expect(loadBuildDraft()).toEqual(draft);
    clearBuildDraft();
    expect(loadBuildDraft()).toBeNull();
  });

  it("discards malformed stored data", () => {
    localStorage.setItem("forgesavant:build-draft", "not-json");
    expect(loadBuildDraft()).toBeNull();
  });
});
