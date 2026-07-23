const { parseNumber } = require('./compatibility.service');

const ANALYTICS_MODEL_VERSION = 'planning-2.0.0';

const classify = (value, thresholds, labels) => {
  if (value >= thresholds[1]) return labels[2];
  if (value >= thresholds[0]) return labels[1];
  return labels[0];
};

const estimatePerformance = (processor, gpu) => {
  if (!processor || !gpu) {
    return {
      status: 'incomplete',
      confidence: 'unavailable',
      model: { version: ANALYTICS_MODEL_VERSION, type: 'deterministic-planning' },
      performance: null,
      assumptions: ['A processor and graphics card are required.'],
    };
  }

  const cores = parseNumber(processor.specifications?.cores) || 4;
  const threads = parseNumber(processor.specifications?.threads) || cores;
  const boostClockGHz = parseNumber(processor.specifications?.boost_clock || processor.specifications?.base_clock) || 1;
  const gpuMemoryGB = parseNumber(gpu.specifications?.memory);
  const gpuTdp = parseNumber(gpu.specifications?.tdp);
  const cpuParallelismIndex = Math.round((cores + Math.max(0, threads - cores) * 0.35) * boostClockGHz * 10);

  return {
    status: 'planning-estimate',
    confidence: 'low',
    model: { version: ANALYTICS_MODEL_VERSION, type: 'deterministic-planning' },
    performance: {
      cpuParallelismIndex,
      gpuMemoryGB,
      gpuBoardPowerWatts: gpuTdp,
      workloadProfile: {
        cpuMultitasking: classify(threads, [12, 24], ['basic', 'balanced', 'high']),
        gpuMemoryCapacity: classify(gpuMemoryGB, [12, 16], ['baseline', 'capable', 'large']),
      },
    },
    inputs: { cores, threads, boostClockGHz, gpuMemoryGB, gpuTdp },
    assumptions: [
      'The CPU parallelism index is dimensionless and is not a benchmark score.',
      'GPU memory and board power are manufacturer specifications, not predicted frame rates.',
      'Application performance, cooling, drivers, memory timings, and workload behavior are not modeled.',
    ],
  };
};

module.exports = { ANALYTICS_MODEL_VERSION, estimatePerformance };
