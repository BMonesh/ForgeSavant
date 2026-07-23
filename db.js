const mongoose = require('mongoose');
require('dotenv').config();

const startDB = async () => {
  const mongoUri = process.env.URI || 'mongodb://127.0.0.1:27017/forgesavant';
  let retries = 3;

  while (retries) {
    try {
      await mongoose.connect(mongoUri);
      console.log('Database initiated: connection successful.');
      break;
    } catch (err) {
      console.error(`Database connection failed: ${err.message}`);
      retries -= 1;
      console.log(`Retries left: ${retries}`);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  if (retries === 0) {
    console.error('Database connection failed after all retries.');
    process.exit(1);
  }
};

const isConnected = () => mongoose.connection.readyState === 1;

module.exports = { startDB, isConnected };
