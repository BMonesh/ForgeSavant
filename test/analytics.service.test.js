const test = require('node:test');
const assert = require('node:assert/strict');
const { ANALYTICS_MODEL_VERSION, estimatePerformance } = require('../services/analytics.service');

const part = (name, specifications) => ({ name, specifications });

test('returns a versioned, explicitly low-confidence planning estimate', () => {
  const result = estimatePerformance(
    part('Ryzen 5', { cores: 6, threads: 12, boost_clock: '4.4 GHz' }),
    part('RTX class GPU', { memory: '12GB GDDR6', tdp: '170W' })
  );

  assert.equal(result.status, 'planning-estimate');
  assert.equal(result.confidence, 'low');
  assert.equal(result.model.version, ANALYTICS_MODEL_VERSION);
  assert.deepEqual(result.performance, {
    cpuParallelismIndex: 356,
    gpuMemoryGB: 12,
    gpuBoardPowerWatts: 170,
    workloadProfile: { cpuMultitasking: 'balanced', gpuMemoryCapacity: 'capable' },
  });
  assert.match(result.assumptions[0], /dimensionless/i);
});

test('reports incomplete analytics when required parts are absent', () => {
  const result = estimatePerformance(null, null);
  assert.equal(result.status, 'incomplete');
  assert.equal(result.performance, null);
});
