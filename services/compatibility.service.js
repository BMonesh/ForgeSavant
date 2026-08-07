const PSU_HEADROOM = 1.2;
const SYSTEM_BASE_POWER_WATTS = 75;
const PSU_TIERS = [450, 550, 650, 750, 850, 1050, 1200, 1600];

const parseNumber = (value) => {
  const match = String(value || "").replace(/,/g, "").match(/\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : 0;
};

const normalize = (value) => String(value || "").trim().toUpperCase();

const chipsetMemoryMap = {
  A320: ["DDR4"],
  B350: ["DDR4"],
  B450: ["DDR4"],
  B550: ["DDR4"],
  X370: ["DDR4"],
  X470: ["DDR4"],
  X570: ["DDR4"],
  A620: ["DDR5"],
  B650: ["DDR5"],
  X670: ["DDR5"],
  X870: ["DDR5"],
  H410: ["DDR4"],
  B460: ["DDR4"],
  Z490: ["DDR4"],
  H510: ["DDR4"],
  B560: ["DDR4"],
  Z590: ["DDR4"],
};

const supportedMemoryTypes = (motherboard) => {
  const specs = motherboard?.specifications || {};
  const explicit = specs.memory_type || specs.ram_type;
  if (explicit) return Array.isArray(explicit) ? explicit.map(normalize) : [normalize(explicit)];

  const chipset = normalize(specs.chipset);
  return chipsetMemoryMap[chipset] || [];
};

const calculatePower = (processor, gpu) => {
  const cpuWatts = parseNumber(processor?.specifications?.tdp);
  const gpuWatts = parseNumber(gpu?.specifications?.tdp);
  const estimatedDraw = cpuWatts + gpuWatts + SYSTEM_BASE_POWER_WATTS;
  const required = Math.ceil(estimatedDraw * PSU_HEADROOM);
  const recommendedPsu = PSU_TIERS.find((tier) => tier >= required) || Math.ceil(required / 50) * 50;
  return { cpuWatts, gpuWatts, estimatedDraw, required, recommendedPsu };
};

const createCheck = (id, label, status, message, evidence = {}) => ({
  id,
  label,
  status,
  message,
  evidence,
});

const evaluateCompatibility = (components = {}) => {
  const { processor, motherboard, gpu, ram, smps, cabinet, primaryStorage, secondaryStorage } = components;
  const checks = [];

  if (processor && motherboard) {
    const cpuSocket = normalize(processor.specifications?.socket);
    const boardSocket = normalize(motherboard.specifications?.socket);
    const passed = Boolean(cpuSocket && boardSocket && cpuSocket === boardSocket);
    checks.push(createCheck(
      "cpu-socket",
      "CPU socket",
      passed ? "pass" : "fail",
      passed ? `${processor.name} and ${motherboard.name} use ${cpuSocket}.` : `${processor.name} requires ${cpuSocket || "an unknown socket"}; ${motherboard.name} uses ${boardSocket || "an unknown socket"}.`,
      { cpuSocket, boardSocket }
    ));
  }

  if (motherboard && ram) {
    const memoryType = normalize(ram.specifications?.type);
    const supported = supportedMemoryTypes(motherboard);
    const status = supported.length === 0 ? "unknown" : supported.includes(memoryType) ? "pass" : "fail";
    checks.push(createCheck(
      "memory-type",
      "Memory generation",
      status,
      status === "unknown"
        ? `${motherboard.name} has no verified memory-generation attribute.`
        : status === "pass"
          ? `${motherboard.name} supports ${memoryType}.`
          : `${motherboard.name} supports ${supported.join(" / ")}, not ${memoryType || "the selected kit"}.`,
      { memoryType, supported }
    ));
  }

  const power = calculatePower(processor, gpu);
  if (processor && gpu && smps) {
    const psuWatts = parseNumber(smps.specifications?.wattage);
    const passed = psuWatts >= power.required;
    checks.push(createCheck(
      "power-budget",
      "Power budget",
      passed ? "pass" : "fail",
      passed ? `${psuWatts} W provides at least 20% planning headroom.` : `${psuWatts} W is below the ${power.required} W minimum planning target.`,
      { psuWatts, ...power }
    ));
  }

  if (motherboard && cabinet) {
    const formFactor = normalize(motherboard.specifications?.form_factor);
    const support = normalize(cabinet.specifications?.motherboard_support);
    const passed = Boolean(formFactor && support.includes(formFactor));
    checks.push(createCheck(
      "case-form-factor",
      "Case fit",
      passed ? "pass" : "fail",
      passed ? `${cabinet.name} lists support for ${formFactor}.` : `${cabinet.name} does not list support for ${formFactor || "the selected board"}.`,
      { formFactor, support }
    ));
  }

  if (primaryStorage) {
    const storageInterface = normalize(primaryStorage.specifications?.interface);
    checks.push(createCheck(
      "primary-storage",
      "Primary storage interface",
      storageInterface === "NVME" ? "pass" : "fail",
      storageInterface === "NVME" ? `${primaryStorage.name} is an NVMe primary drive.` : `${primaryStorage.name} is ${storageInterface || "an unknown interface"}; the primary slot expects NVMe.`,
      { storageInterface }
    ));
  }

  if (secondaryStorage) {
    const storageInterface = normalize(secondaryStorage.specifications?.interface);
    checks.push(createCheck(
      "secondary-storage",
      "Secondary storage interface",
      storageInterface === "SATA" ? "pass" : "fail",
      storageInterface === "SATA" ? `${secondaryStorage.name} is a SATA secondary drive.` : `${secondaryStorage.name} is ${storageInterface || "an unknown interface"}; the secondary slot expects SATA.`,
      { storageInterface }
    ));
  }

  const failed = checks.filter((check) => check.status === "fail");
  const unknown = checks.filter((check) => check.status === "unknown");
  // A secondary SATA drive is optional; a build is complete once every
  // selected/required compatibility surface has been evaluated.
  const expectedChecks = secondaryStorage ? 6 : 5;
  const status = failed.length
    ? "incompatible"
    : checks.length < expectedChecks || unknown.length
      ? "incomplete"
      : "compatible";

  return {
    status,
    checks,
    summary: {
      passed: checks.filter((check) => check.status === "pass").length,
      failed: failed.length,
      unknown: unknown.length,
      evaluated: checks.length,
      expected: expectedChecks,
    },
    power,
    engine: { version: "1.0.0", evaluatedAt: new Date().toISOString() },
  };
};

module.exports = {
  calculatePower,
  evaluateCompatibility,
  parseNumber,
  supportedMemoryTypes,
};
