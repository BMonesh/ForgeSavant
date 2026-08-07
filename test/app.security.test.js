const test = require('node:test');
const assert = require('node:assert/strict');
const request = require('supertest');
const app = require('../app');
const Processor = require('../models/processor.model');

test('exposes liveness with hardened response headers', async () => {
  const response = await request(app).get('/live').expect(200);

  assert.equal(response.body.status, 'alive');
  assert.equal(response.headers['x-content-type-options'], 'nosniff');
  assert.equal(response.headers['x-powered-by'], undefined);
});

test('allows configured local origins', async () => {
  const response = await request(app)
    .get('/live')
    .set('Origin', 'http://localhost:5173')
    .expect(200);

  assert.equal(response.headers['access-control-allow-origin'], 'http://localhost:5173');
});

test('rejects unconfigured origins without leaking an internal error', async () => {
  const response = await request(app)
    .get('/live')
    .set('Origin', 'https://untrusted.example')
    .expect(403);

  assert.equal(response.body.error, 'Origin is not allowed');
});

test('returns a consistent JSON 404', async () => {
  const response = await request(app).get('/not-a-route').expect(404);
  assert.deepEqual(response.body, { error: 'Route not found' });
});

test('rejects oversized JSON payloads', async () => {
  const response = await request(app)
    .post('/login')
    .send({ email: 'user@example.com', password: 'x'.repeat(1024 * 1024 + 1) })
    .expect(413);

  assert.equal(response.body.error, 'request entity too large');
});

test('protects saved builds when no bearer token is supplied', async () => {
  const response = await request(app).get('/saves2').expect(401);
  assert.equal(response.body.error, 'Authentication required');
});

test('validates compatibility component ids before database access', async () => {
  const response = await request(app)
    .post('/api/v1/compatibility/evaluate')
    .send({ componentIds: { processor: 'not-an-object-id' } })
    .expect(400);

  assert.equal(response.body.error, 'Invalid component id for processor');
});

test('validates analytics component ids before database access', async () => {
  const response = await request(app)
    .post('/api/v1/analytics/estimate')
    .send({ componentIds: { processor: 'not-an-object-id', gpu: 'also-invalid' } })
    .expect(400);

  assert.equal(response.body.error, 'Invalid component id for processor');
});

test('legacy catalog routes redact operator identity and classify sample pricing', async (t) => {
  t.mock.method(Processor, 'find', async () => [{
    _id: '507f1f77bcf86cd799439011',
    name: 'Catalog CPU',
    provenance: {
      source: 'seed',
      data_status: 'sample',
      imported_by: 'operator@example.com',
    },
  }]);

  const response = await request(app).get('/CPU').expect(200);
  assert.equal(response.body[0].pricing.status, 'sample');
  assert.equal(response.body[0].provenance.imported_by, undefined);
});

test('legacy catalog detail routes reject invalid object ids', async () => {
  const response = await request(app).get('/CPU/not-an-object-id').expect(400);
  assert.equal(response.body.error, 'Invalid resource id');
});
