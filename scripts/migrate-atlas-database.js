require('dotenv').config();

const { MongoClient } = require('mongodb');

const SOURCE_DATABASE = process.env.MONGODB_SOURCE_DATABASE || 'test';
const TARGET_DATABASE = process.env.MONGODB_TARGET_DATABASE || 'forgesavant';

const main = async () => {
  if (!process.env.URI) throw new Error('URI is required');
  if (SOURCE_DATABASE === TARGET_DATABASE) {
    throw new Error('Source and target database names must be different');
  }

  const client = new MongoClient(process.env.URI);
  await client.connect();

  try {
    const source = client.db(SOURCE_DATABASE);
    const target = client.db(TARGET_DATABASE);
    const collections = await source.listCollections({}, { nameOnly: true }).toArray();
    const summary = [];

    const occupiedTargets = [];
    for (const { name } of collections) {
      const count = await target.collection(name).countDocuments({});
      if (count > 0) occupiedTargets.push(`${name} (${count})`);
    }
    if (occupiedTargets.length > 0) {
      throw new Error(
        `Refusing migration because target collections are not empty: ${occupiedTargets.join(', ')}`,
      );
    }

    for (const { name } of collections) {
      const sourceCollection = source.collection(name);
      const targetCollection = target.collection(name);
      const documents = await sourceCollection.find({}).toArray();

      if (documents.length > 0) await targetCollection.insertMany(documents, { ordered: true });

      const indexes = await sourceCollection.indexes();
      for (const index of indexes) {
        if (index.name === '_id_') continue;
        const { key, name: indexName, v, ns, background, ...options } = index;
        await targetCollection.createIndex(key, { ...options, name: indexName });
      }

      summary.push({ collection: name, copied: documents.length });
    }

    console.log(JSON.stringify({ source: SOURCE_DATABASE, target: TARGET_DATABASE, collections: summary }, null, 2));
  } finally {
    await client.close();
  }
};

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
