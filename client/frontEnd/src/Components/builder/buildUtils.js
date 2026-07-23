export const SYSTEM_BASE_POWER_WATTS = 75;
export const PSU_HEADROOM = 1.2;
export const PSU_TIERS = [450, 550, 650, 750, 850, 1050, 1200, 1600];

export const stepOrder = [
  "platform",
  "processor",
  "motherboard",
  "gpu",
  "primaryStorage",
  "secondaryStorage",
  "ram",
  "smps",
  "cabinet",
  "review",
];

export const stepLabels = {
  platform: "Platform",
  processor: "CPU",
  motherboard: "Motherboard",
  gpu: "GPU",
  primaryStorage: "Primary storage",
  secondaryStorage: "Secondary storage",
  ram: "RAM",
  smps: "PSU",
  cabinet: "Cabinet",
  review: "Review build",
};

export const emptySelection = {
  platform: "",
  processor: null,
  motherboard: null,
  gpu: null,
  primaryStorage: null,
  secondaryStorage: null,
  skipSecondaryStorage: false,
  ram: null,
  smps: null,
  cabinet: null,
};

export const parseWatts = (value) => {
  const match = String(value || "").match(/(\d+)/);
  return match ? Number(match[1]) : 0;
};

export const formatPrice = (value) => {
  const amount = Number(value || 0);
  return amount ? `INR ${amount.toLocaleString("en-IN")}` : "Price pending";
};

const roundToPsuTier = (watts) =>
  PSU_TIERS.find((tier) => tier >= watts) || Math.ceil(watts / 50) * 50;

export const calculateRecommendedPsu = (processor, gpu) => {
  const cpuTdp = parseWatts(processor?.specifications?.tdp);
  const gpuTdp = parseWatts(gpu?.specifications?.tdp);
  const estimatedDraw = cpuTdp + gpuTdp + SYSTEM_BASE_POWER_WATTS;

  return processor && gpu
    ? roundToPsuTier(Math.ceil(estimatedDraw * PSU_HEADROOM))
    : 0;
};

const socketRamTypes = {
  AM4: ["DDR4"],
  AM5: ["DDR5"],
  "LGA 1700": ["DDR4", "DDR5"],
  "LGA 1200": ["DDR4"],
  "LGA 1151": ["DDR4"],
};

export const inferSupportedRamTypes = (motherboard) => {
  const explicitRamType = motherboard?.specifications?.memory_type || motherboard?.specifications?.ram_type;
  if (explicitRamType) {
    return [explicitRamType];
  }

  const socket = motherboard?.specifications?.socket;
  return socketRamTypes[socket] || ["DDR4", "DDR5"];
};

export const estimatePerformance = (processor, gpu) => {
  if (!processor || !gpu) {
    return null;
  }

  const cores = Number(processor?.specifications?.cores) || 4;
  const threads = Number(processor?.specifications?.threads) || cores;
  const boostClockGHz = Number.parseFloat(processor?.specifications?.boost_clock || processor?.specifications?.base_clock) || 1;
  const gpuMemoryGB = parseWatts(gpu?.specifications?.memory);
  const gpuTdp = parseWatts(gpu?.specifications?.tdp);

  return {
    cpuParallelismIndex: Math.round((cores + Math.max(0, threads - cores) * 0.35) * boostClockGHz * 10),
    gpuMemoryGB,
    gpuBoardPowerWatts: gpuTdp,
    confidence: "low",
  };
};

export const isStepSelected = (selection, stepId) => {
  if (stepId === "review") {
    return isBuildComplete(selection);
  }

  if (stepId === "secondaryStorage") return Boolean(selection.secondaryStorage || selection.skipSecondaryStorage);
  return Boolean(selection[stepId]);
};

export const isBuildComplete = (selection) =>
  Boolean(
    selection.platform &&
      selection.processor &&
      selection.motherboard &&
      selection.gpu &&
      selection.primaryStorage &&
      selection.ram &&
      selection.smps &&
      selection.cabinet
  );

export const resetAfterStep = (selection, stepId) => {
  const stepIndex = stepOrder.indexOf(stepId);
  return stepOrder.reduce(
    (nextSelection, id, index) => {
      if (index > stepIndex && id !== "review") {
        nextSelection[id] = emptySelection[id];
      }
      return nextSelection;
    },
    { ...selection, skipSecondaryStorage: stepOrder.indexOf(stepId) < stepOrder.indexOf("secondaryStorage") ? false : selection.skipSecondaryStorage }
  );
};

export const getStepDependency = (selection, stepId) => {
  if (stepId === "platform") return "";
  if (stepId === "processor") return selection.platform ? "" : "Choose platform first";
  if (stepId === "motherboard") return selection.processor ? "" : "Choose CPU first";
  if (stepId === "gpu") return selection.motherboard ? "" : "Choose motherboard first";
  if (stepId === "primaryStorage") return selection.gpu ? "" : "Choose GPU first";
  if (stepId === "secondaryStorage") return selection.primaryStorage ? "" : "Choose primary storage first";
  if (stepId === "ram") return selection.secondaryStorage || selection.skipSecondaryStorage ? "" : "Choose or skip secondary storage first";
  if (stepId === "smps") return selection.ram ? "" : "Choose RAM first";
  if (stepId === "cabinet") return selection.smps ? "" : "Choose PSU first";
  if (stepId === "review") return isBuildComplete(selection) ? "" : "Complete all component steps";
  return "";
};

export const canVisitStep = (selection, stepId) => !getStepDependency(selection, stepId);

export const getItemsForStep = (stepId, selection, catalog) => {
  if (stepId === "platform") {
    return [
      {
        _id: "AMD",
        name: "AMD",
        manufacturer: "AMD",
        type: "CPU platform",
        specifications: { rule: "Filters processors to AMD socket families" },
      },
      {
        _id: "Intel",
        name: "Intel",
        manufacturer: "Intel",
        type: "CPU platform",
        specifications: { rule: "Filters processors to Intel socket families" },
      },
    ];
  }

  if (stepId === "processor") {
    return catalog.cpu.filter(
      (processor) =>
        processor.manufacturer?.toLowerCase() === selection.platform.toLowerCase()
    );
  }

  if (stepId === "motherboard") {
    return catalog.motherboard.filter(
      (motherboard) =>
        motherboard.specifications?.socket ===
        selection.processor?.specifications?.socket
    );
  }

  if (stepId === "gpu") {
    return catalog.gpu;
  }

  if (stepId === "primaryStorage") {
    return catalog.storage.filter(
      (storage) => storage.specifications?.interface === "NVMe"
    );
  }

  if (stepId === "secondaryStorage") {
    return catalog.storage.filter(
      (storage) => storage.specifications?.interface === "SATA"
    );
  }

  if (stepId === "ram") {
    const allowedTypes = inferSupportedRamTypes(selection.motherboard);
    return catalog.ram.filter((ram) =>
      allowedTypes.includes(ram.specifications?.type)
    );
  }

  if (stepId === "smps") {
    const target = calculateRecommendedPsu(selection.processor, selection.gpu);
    return catalog.smps.filter(
      (smps) => target > 0 && parseWatts(smps.specifications?.wattage) >= target
    );
  }

  if (stepId === "cabinet") {
    const boardFormFactor = selection.motherboard?.specifications?.form_factor;
    return catalog.cabinet.filter((cabinet) =>
      cabinet.specifications?.motherboard_support?.includes(boardFormFactor)
    );
  }

  return [];
};

export const getSelectedForStep = (selection, stepId) => {
  if (stepId === "platform") {
    return selection.platform
      ? {
          _id: selection.platform,
          name: selection.platform,
          manufacturer: selection.platform,
          type: "CPU platform",
          specifications: {
            rule: `Filters processors to ${selection.platform} socket families`,
          },
        }
      : null;
  }

  return selection[stepId] || null;
};

export const getItemUtilityFacts = (item, stepId) => {
  const specs = item?.specifications || {};
  if (!item) return [];

  const factsByStep = {
    platform: [item.type, specs.rule],
    processor: [
      `${specs.cores || "-"} cores`,
      `${specs.threads || "-"} threads`,
      specs.socket,
      specs.tdp,
    ],
    motherboard: [specs.socket, specs.chipset, specs.form_factor],
    gpu: [specs.memory, specs.tdp, `${specs.core_count || "-"} cores`],
    primaryStorage: [specs.capacity, specs.interface, specs.speed],
    secondaryStorage: [specs.capacity, specs.interface, specs.speed],
    ram: [specs.capacity, specs.type, specs.speed],
    smps: [specs.wattage, specs.efficiency, specs.fan_size],
    cabinet: [specs.form_factor, specs.motherboard_support],
  };

  return (factsByStep[stepId] || [item.type, item.manufacturer]).filter(Boolean);
};

export const getRuleEvidence = (selection, stepId) => {
  if (stepId === "platform") return "Platform controls which CPU family appears next.";
  if (stepId === "processor" && selection.platform) return `Showing ${selection.platform} processors only.`;
  if (stepId === "motherboard" && selection.processor) {
    return `Motherboards must match CPU socket ${selection.processor.specifications?.socket}.`;
  }
  if (stepId === "gpu" && selection.motherboard) return "GPU selection is open after motherboard compatibility is locked.";
  if (stepId === "primaryStorage") return "Primary drive list is limited to NVMe storage.";
  if (stepId === "secondaryStorage") return "Secondary drive list is limited to SATA storage.";
  if (stepId === "ram" && selection.motherboard) {
    return `RAM must match supported type: ${inferSupportedRamTypes(selection.motherboard).join(", ")}.`;
  }
  if (stepId === "smps") {
    const target = calculateRecommendedPsu(selection.processor, selection.gpu);
    return target
      ? `PSU must be at or above ${target}W including headroom.`
      : "PSU target appears after CPU and GPU are selected.";
  }
  if (stepId === "cabinet" && selection.motherboard) {
    return `Cabinet must support ${selection.motherboard.specifications?.form_factor}.`;
  }
  return getStepDependency(selection, stepId) || "Complete the current step to continue.";
};

export const getRailSteps = (selection, currentStepId) =>
  stepOrder
    .filter((stepId) => stepId !== "review")
    .map((stepId, index) => {
      const selected = getSelectedForStep(selection, stepId);
      const skipped = stepId === "secondaryStorage" && selection.skipSecondaryStorage;
      const dependency = getStepDependency(selection, stepId);
      const status = stepId === currentStepId
        ? "current"
        : selected || skipped
          ? "compatible"
          : dependency
            ? "blocked"
            : "pending";

      return {
        id: stepId,
        index: index + 1,
        label: stepLabels[stepId],
        value: skipped ? "Skipped" : selected?.name || "Not selected",
        meta: selected
          ? getItemUtilityFacts(selected, stepId).slice(0, 2).join(" / ")
          : skipped
            ? "Optional drive not included"
          : dependency || getRuleEvidence(selection, stepId),
        status,
        canRevisit: canVisitStep(selection, stepId),
      };
    });

export const getSpecRows = (part) =>
  Object.entries(part?.specifications || {}).map(([key, value]) => ({
    label: key.replaceAll("_", " "),
    value: Array.isArray(value) ? value.join(", ") : String(value),
  }));

export const getBuildTotal = (selection) =>
  [
    selection.processor,
    selection.motherboard,
    selection.gpu,
    selection.primaryStorage,
    selection.secondaryStorage,
    selection.ram,
    selection.smps,
    selection.cabinet,
  ].reduce((sum, item) => sum + Number(item?.price || 0), 0);

export const hydrateSavedBuild = (savedBuild, catalog) => {
  const findPart = (items, name, id) =>
    items.find((item) => id && item._id === id)
      || items.find((item) => item.name === name || item.identity?.aliases?.includes(name))
      || null;
  const ids = savedBuild.componentIds || {};
  const processor = findPart(catalog.cpu || [], savedBuild.cpu, ids.processor);

  return {
    platform: processor?.manufacturer || "",
    processor,
    motherboard: findPart(catalog.motherboard || [], savedBuild.motherboard, ids.motherboard),
    gpu: findPart(catalog.gpu || [], savedBuild.gpu, ids.gpu),
    primaryStorage: findPart(catalog.storage || [], savedBuild.primaryStorage, ids.primaryStorage),
    secondaryStorage: findPart(catalog.storage || [], savedBuild.secondaryStorage, ids.secondaryStorage),
    skipSecondaryStorage: !savedBuild.secondaryStorage,
    ram: findPart(catalog.ram || [], savedBuild.ram, ids.ram),
    smps: findPart(catalog.smps || [], savedBuild.powerSupply, ids.smps),
    cabinet: findPart(catalog.cabinet || [], savedBuild.cabinet, ids.cabinet),
  };
};

export const getFirstIncompleteStep = (selection) =>
  stepOrder.find((stepId) => stepId !== "review" && !isStepSelected(selection, stepId)) || "review";
