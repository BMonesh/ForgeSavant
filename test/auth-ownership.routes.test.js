const test = require('node:test');
const assert = require('node:assert/strict');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const request = require('supertest');

process.env.JWT_SECRET = 'test-secret-that-is-long-enough-for-route-tests';

const app = require('../app');
const User = require('../models/user.model');
const { saves, saves2 } = require('../models/saves.model');
const Processor = require('../models/processor.model');
const GraphicsCard = require('../models/graphicsCard.model');
const Motherboard = require('../models/motherboard.model');
const RAM = require('../models/ram.model');
const Storage = require('../models/storage.model');
const SMPS = require('../models/smps.model');
const Cabinet = require('../models/cabinet.model');

const userId = '507f1f77bcf86cd799439011';
const buildId = '507f1f77bcf86cd799439012';
const user = { id: userId, _id: userId, fullname: 'Test Builder', email: 'builder@example.com' };
const token = jwt.sign({ user: { id: userId, email: user.email } }, process.env.JWT_SECRET, {
  algorithm: 'HS256',
  expiresIn: '15m',
});

const mockAuthenticatedUser = (t) => {
  t.mock.method(User, 'findById', () => ({ select: async () => user }));
};

test('login returns a signed token and serialized user', async (t) => {
  const password = 'correct-horse-battery-staple';
  const passwordHash = await bcrypt.hash(password, 4);
  t.mock.method(User, 'findOne', async () => ({ ...user, password: passwordHash }));

  const response = await request(app)
    .post('/login')
    .send({ email: user.email, password })
    .expect(200);

  const decoded = jwt.verify(response.body.token, process.env.JWT_SECRET, { algorithms: ['HS256'] });
  assert.equal(decoded.user.id, userId);
  assert.deepEqual(response.body.user, { id: userId, fullname: user.fullname, email: user.email, isAdmin: false });
});

test('saved-build listing is scoped to the authenticated email', async (t) => {
  mockAuthenticatedUser(t);
  let query;
  t.mock.method(saves2, 'find', async (filter) => {
    query = filter;
    return [{ _id: buildId, email: user.email, cpu: 'Ryzen' }];
  });

  const response = await request(app)
    .get('/saves2')
    .set('Authorization', `Bearer ${token}`)
    .expect(200);

  assert.deepEqual(query, { email: user.email });
  assert.equal(response.body[0].email, user.email);
});

test('saved-build deletion includes owner identity in its database filter', async (t) => {
  mockAuthenticatedUser(t);
  let query;
  t.mock.method(saves2, 'findOneAndDelete', async (filter) => {
    query = filter;
    return null;
  });

  await request(app)
    .delete(`/delsaves/${buildId}`)
    .set('Authorization', `Bearer ${token}`)
    .expect(404);

  assert.deepEqual(query, { _id: buildId, email: user.email });
});

test('expired tokens are rejected before database access', async () => {
  const expired = jwt.sign({ user: { id: userId, email: user.email } }, process.env.JWT_SECRET, {
    algorithm: 'HS256',
    expiresIn: '-1s',
  });

  const response = await request(app)
    .get('/saves2')
    .set('Authorization', `Bearer ${expired}`)
    .expect(401);

  assert.equal(response.body.error, 'Invalid or expired token');
});

test('saved-build writes require catalog component ids', async (t) => {
  mockAuthenticatedUser(t);
  const response = await request(app)
    .put(`/saves/${buildId}`)
    .set('Authorization', `Bearer ${token}`)
    .send({ cpu: 'Untrusted label' })
    .expect(400);

  assert.equal(response.body.error, 'Catalog component ids are required');
});

test('saved-build names and evidence are derived from catalog records', async (t) => {
  mockAuthenticatedUser(t);
  const ids = {
    processor: '507f1f77bcf86cd799439021', motherboard: '507f1f77bcf86cd799439022',
    gpu: '507f1f77bcf86cd799439023', primaryStorage: '507f1f77bcf86cd799439024',
    ram: '507f1f77bcf86cd799439025', smps: '507f1f77bcf86cd799439026',
    cabinet: '507f1f77bcf86cd799439027',
  };
  const records = new Map([
    [Processor, { name: 'Catalog CPU', specifications: { socket: 'AM5', cores: 6, threads: 12, tdp: '65W' } }],
    [Motherboard, { name: 'Catalog Board', specifications: { socket: 'AM5', memory_type: 'DDR5', form_factor: 'ATX' } }],
    [GraphicsCard, { name: 'Catalog GPU', specifications: { tdp: '185W', core_count: 2048 } }],
    [Storage, { name: 'Catalog NVMe', specifications: { interface: 'NVMe' } }],
    [RAM, { name: 'Catalog RAM', specifications: { type: 'DDR5' } }],
    [SMPS, { name: 'Catalog PSU', specifications: { wattage: '450W' } }],
    [Cabinet, { name: 'Catalog Case', image_url: '', specifications: { motherboard_support: 'ATX, Micro-ATX' } }],
  ]);
  for (const [Model, record] of records) {
    t.mock.method(Model, 'findById', () => ({ lean: async () => record }));
  }
  let stored;
  t.mock.method(saves.prototype, 'save', async function save() { stored = this.toObject(); return this; });

  await request(app)
    .post('/saves')
    .set('Authorization', `Bearer ${token}`)
    .send({ cpu: 'Forged CPU', gpu: 'Forged GPU', componentIds: ids })
    .expect(201);

  assert.equal(stored.cpu, 'Catalog CPU');
  assert.equal(stored.gpu, 'Catalog GPU');
  assert.equal(stored.secondaryStorage, '');
  assert.equal(stored.compatibility.status, 'compatible');
  assert.equal(stored.analytics.performance.cpuParallelismIndex, 81);
  assert.equal(stored.analytics.performance.gpuMemoryGB, 0);
});
