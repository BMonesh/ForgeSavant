require('dotenv').config();

const app = require('./app');
const { startDB } = require('./db');
const { assertRuntimeConfig } = require('./services/runtime-config.service');

const PORT = process.env.PORT || 5000;

const startServer = async () => {
  assertRuntimeConfig();
  await startDB();
  const server = app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') console.error(`Port ${PORT} is already in use`);
    else console.error('An error occurred while starting the server:', err);
  });

  const shutdown = (signal) => {
    console.log(`${signal} received. Closing HTTP server.`);
    server.close(() => process.exit(0));
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  return server;
};

if (require.main === module) {
  startServer().catch((err) => {
    console.error('An error occurred while starting the server:', err);
    process.exit(1);
  });
}

module.exports = { startServer };
