const PLACEHOLDER_PATTERN = /replace-with|change-me|example-secret/i;

const validateRuntimeConfig = (env = process.env, nodeVersion = process.versions.node) => {
  const errors = [];
  const [major, minor] = String(nodeVersion).split('.').map(Number);
  if (major < 20 || (major === 20 && minor < 19)) {
    errors.push('Node.js 20.19 or newer is required');
  }

  if (env.NODE_ENV !== 'production') return errors;

  if (!env.URI) errors.push('URI is required in production');
  if (!env.JWT_SECRET || env.JWT_SECRET.length < 32 || PLACEHOLDER_PATTERN.test(env.JWT_SECRET)) {
    errors.push('JWT_SECRET must be a non-placeholder secret of at least 32 characters');
  }

  const origins = String(env.ALLOWED_ORIGINS || '').split(',').map((value) => value.trim()).filter(Boolean);
  if (!origins.length) {
    errors.push('ALLOWED_ORIGINS is required in production');
  } else if (origins.some((origin) => !origin.startsWith('https://'))) {
    errors.push('Every production ALLOWED_ORIGINS entry must use HTTPS');
  }

  return errors;
};

const assertRuntimeConfig = (env = process.env, nodeVersion = process.versions.node) => {
  const errors = validateRuntimeConfig(env, nodeVersion);
  if (errors.length) {
    throw new Error(`Invalid runtime configuration:\n- ${errors.join('\n- ')}`);
  }
};

module.exports = { assertRuntimeConfig, validateRuntimeConfig };
