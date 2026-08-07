const test = require("node:test");
const assert = require("node:assert/strict");

const { upsertMany } = require("../scripts/seed-local-data");

const fakeModel = (records) => {
  const state = records.map((record) => ({ ...record }));

  return {
    state,
    find() {
      return {
        lean: async () => state.map(({ _id, name }) => ({ _id, name })),
      };
    },
    async updateOne(filter, update) {
      const record = state.find(({ _id }) => _id === filter._id);
      Object.assign(record, update.$set);
    },
    async create(document) {
      const created = { _id: String(state.length + 1), ...document };
      state.push(created);
      return created;
    },
  };
};

test("upsertMany updates a normalized identity instead of creating a duplicate", async () => {
  const Model = fakeModel([{ _id: "existing", name: "NVIDIA GeForce RTX 4070 Graphics Card", price: 1 }]);

  await upsertMany(Model, [{ name: "NVIDIA GeForce RTX 4070", price: 2 }]);

  assert.equal(Model.state.length, 1);
  assert.equal(Model.state[0].price, 2);
});

test("upsertMany creates a new product once and remains idempotent", async () => {
  const Model = fakeModel([]);
  const documents = [{ name: "AMD Ryzen 7 7700X Processor", price: 30000 }];

  await upsertMany(Model, documents);
  await upsertMany(Model, documents);

  assert.equal(Model.state.length, 1);
  assert.equal(Model.state[0].name, documents[0].name);
});

test("upsertMany preserves unmatched verified products when local preserve mode is enabled", async () => {
  const previous = process.env.SEED_PRESERVE_EXISTING;
  process.env.SEED_PRESERVE_EXISTING = "1";
  const Model = fakeModel([{ _id: "verified", name: "ASUS TUF Gaming GeForce RTX 4070 OC Edition 12GB" }]);

  try {
    await upsertMany(Model, [{ name: "NVIDIA GeForce RTX 4070", price: 49999 }]);
    assert.equal(Model.state.length, 1);
    assert.equal(Model.state[0]._id, "verified");
  } finally {
    if (previous === undefined) delete process.env.SEED_PRESERVE_EXISTING;
    else process.env.SEED_PRESERVE_EXISTING = previous;
  }
});
