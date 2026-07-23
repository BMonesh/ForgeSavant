const test = require('node:test');
const assert = require('node:assert/strict');
const { assertRuntimeConfig, validateRuntimeConfig } = require('../services/runtime-config.service');

test('allows local development defaults on a supported Node release', () => {
  assert.deepEqual(validateRuntimeConfig({ NODE_ENV: 'development' }, '20.19.0'), []);
});

test('rejects unsupported Node releases', () => {
  assert.deepEqual(validateRuntimeConfig({ NODE_ENV: 'development' }, '20.18.0'), [
    'Node.js 20.19 or newer is required',
  ]);
});

test('requires explicit secure production settings', () => {
  const errors = validateRuntimeConfig({
    NODE_ENV: 'production',
    JWT_SECRET: 'replace-with-a-secure-random-secret',
    ALLOWED_ORIGINS: 'http://localhost:5173',
  }, '22.12.0');

  assert.equal(errors.length, 3);
  assert.match(errors.join(' '), /URI is required/);
  assert.match(errors.join(' '), /JWT_SECRET/);
  assert.match(errors.join(' '), /HTTPS/);
});

test('accepts an explicit production configuration', () => {
  assert.doesNotThrow(() => assertRuntimeConfig({
    NODE_ENV: 'production',
    URI: 'mongodb+srv://service.invalid/forgesavant',
    JWT_SECRET: 'a-random-production-secret-with-32-characters',
    ALLOWED_ORIGINS: 'https://app.forgesavant.example',
  }, '22.12.0'));
});
