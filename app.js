const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const fs = require('node:fs');
const path = require('node:path');
const { rateLimit } = require('express-rate-limit');
const { isConnected } = require('./db');
const route = require('./routes/routes.js');
const apiV1Routes = require('./routes/api-v1.routes.js');

const app = express();

const allowedOrigins = (process.env.ALLOWED_ORIGINS || process.env.RENDER_EXTERNAL_URL || 'http://localhost:5173,http://127.0.0.1:5173')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

if (process.env.NODE_ENV === 'production') {
  app.set('trust proxy', 1);
}

app.disable('x-powered-by');
app.use(helmet({ crossOriginResourcePolicy: false }));
app.use(
  cors({
    origin(origin, callback) {
      if (!origin || allowedOrigins.includes(origin)) return callback(null, true);
      const error = new Error('Origin is not allowed');
      error.statusCode = 403;
      return callback(error);
    },
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
  })
);
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true, limit: '1mb' }));

const apiLimiter = rateLimit({
  windowMs: Number(process.env.RATE_LIMIT_WINDOW_MS) || 15 * 60 * 1000,
  limit: Number(process.env.RATE_LIMIT_MAX) || 300,
  standardHeaders: 'draft-8',
  legacyHeaders: false,
});

const authLimiter = rateLimit({
  windowMs: Number(process.env.AUTH_RATE_LIMIT_WINDOW_MS) || 15 * 60 * 1000,
  limit: Number(process.env.AUTH_RATE_LIMIT_MAX) || 30,
  standardHeaders: 'draft-8',
  legacyHeaders: false,
  message: { error: 'Too many authentication attempts. Try again later.' },
});

app.use(['/login', '/signup', '/googleLogin', '/googleSignup'], authLimiter);
app.use('/api', apiLimiter);
app.use('/api/v1', apiV1Routes);
app.use('/', route);

const frontendDistPath = path.join(__dirname, 'client', 'frontEnd', 'dist');
const frontendIndexPath = path.join(frontendDistPath, 'index.html');
const servesProductionFrontend = process.env.NODE_ENV === 'production' && fs.existsSync(frontendIndexPath);

if (servesProductionFrontend) {
  app.use(express.static(frontendDistPath, { index: false, maxAge: '1d' }));
}

app.get('/', (req, res) => {
  if (servesProductionFrontend) return res.sendFile(frontendIndexPath);
  res.send(isConnected() ? 'Welcome to ForgeSavant API!' : "Server isn't connected to the database yet.");
});

app.get('/live', (req, res) => {
  res.json({ status: 'alive' });
});

app.get('/health', (req, res) => {
  res.json({
    status: isConnected() ? 'ok' : 'degraded',
    database: isConnected() ? 'connected' : 'disconnected',
    uptime: process.uptime(),
  });
});

app.get('/ready', (req, res) => {
  const ready = isConnected();
  res.status(ready ? 200 : 503).json({ status: ready ? 'ready' : 'not-ready' });
});

if (servesProductionFrontend) {
  app.get('*', (req, res, next) => {
    if (req.path === '/api' || req.path.startsWith('/api/')) return next();
    if (!req.accepts('html')) return next();
    return res.sendFile(frontendIndexPath);
  });
}

app.use((req, res) => {
  res.status(404).json({ error: 'Route not found' });
});

app.use((err, req, res, next) => {
  if (res.headersSent) return next(err);
  const statusCode = err.statusCode || (err.type === 'entity.too.large' ? 413 : 500);
  if (statusCode >= 500) console.error('Unhandled request error:', err);
  return res.status(statusCode).json({
    error: statusCode >= 500 ? 'Internal Server Error' : err.message,
  });
});

module.exports = app;
