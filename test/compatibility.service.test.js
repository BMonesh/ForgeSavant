const test = require("node:test");
const assert = require("node:assert/strict");
const { calculatePower, evaluateCompatibility, supportedMemoryTypes } = require("../services/compatibility.service");

const part = (name, specifications) => ({ name, specifications });

test("evaluates a complete compatible build from catalog attributes", () => {
  const result = evaluateCompatibility({
    processor: part("Ryzen 5", { socket: "AM5", tdp: "65W" }),
    motherboard: part("B650 board", { socket: "AM5", chipset: "B650", memory_type: "DDR5", form_factor: "ATX" }),
    gpu: part("GPU", { tdp: "220W" }),
    ram: part("DDR5 kit", { type: "DDR5" }),
    smps: part("650W PSU", { wattage: "650W" }),
    cabinet: part("ATX case", { motherboard_support: "ATX, Micro-ATX" }),
    primaryStorage: part("NVMe", { interface: "NVMe" }),
    secondaryStorage: part("SATA", { interface: "SATA" }),
  });

  assert.equal(result.status, "compatible");
  assert.equal(result.summary.failed, 0);
  assert.equal(result.summary.passed, 6);
  assert.equal(result.power.recommendedPsu, 450);
});

test("reports socket, memory, power, storage and case failures", () => {
  const result = evaluateCompatibility({
    processor: part("Intel CPU", { socket: "LGA 1700", tdp: "125W" }),
    motherboard: part("AM5 board", { socket: "AM5", memory_type: "DDR5", form_factor: "ATX" }),
    gpu: part("GPU", { tdp: "350W" }),
    ram: part("DDR4 kit", { type: "DDR4" }),
    smps: part("450W PSU", { wattage: "450W" }),
    cabinet: part("Small case", { motherboard_support: "Mini-ITX" }),
    primaryStorage: part("SATA", { interface: "SATA" }),
    secondaryStorage: part("NVMe", { interface: "NVMe" }),
  });

  assert.equal(result.status, "incompatible");
  assert.equal(result.summary.failed, 6);
});

test("treats secondary storage as optional", () => {
  const result = evaluateCompatibility({
    processor: part("Ryzen 5", { socket: "AM4", tdp: "65W" }),
    motherboard: part("B550 board", { socket: "AM4", memory_type: "DDR4", form_factor: "ATX" }),
    gpu: part("GPU", { tdp: "165W" }),
    ram: part("DDR4 kit", { type: "DDR4" }),
    smps: part("550W PSU", { wattage: "550W" }),
    cabinet: part("ATX case", { motherboard_support: "ATX, Micro-ATX" }),
    primaryStorage: part("NVMe", { interface: "NVMe" }),
  });

  assert.equal(result.status, "compatible");
  assert.equal(result.summary.expected, 5);
  assert.equal(result.summary.passed, 5);
});

test("does not guess memory support when motherboard evidence is missing", () => {
  assert.deepEqual(supportedMemoryTypes(part("Unknown board", { socket: "LGA 1700", chipset: "B760" })), []);
});

test("rounds power recommendations to a known PSU tier", () => {
  assert.deepEqual(
    calculatePower(part("CPU", { tdp: "125 W" }), part("GPU", { tdp: "300W" })),
    { cpuWatts: 125, gpuWatts: 300, estimatedDraw: 500, required: 600, recommendedPsu: 650 }
  );
});
