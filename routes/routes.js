const express = require("express");
const mongoose = require("mongoose");
const router = express.Router();
const User = require('../models/user.model');
const { saves, saves2 } = require('../models/saves.model');
const bcrypt = require('bcryptjs');
const { check, validationResult } = require('express-validator');
const jwt = require('jsonwebtoken');
const { OAuth2Client } = require('google-auth-library');
const { authenticate } = require('../middleware/auth');
const { evaluateCompatibility } = require('../services/compatibility.service');
const { estimatePerformance } = require('../services/analytics.service');
const { isAdminEmail } = require('../services/admin-access.service');
const { presentCatalogItem } = require('../services/catalog-provenance.service');
const { recordBuildOutcome } = require('../services/product-analytics.service');


const Processor = require("../models/processor.model");
const GraphicsCard = require("../models/graphicsCard.model")
const Motherboard = require("../models/motherboard.model")
const RAM = require("../models/ram.model")
const Storage = require("../models/storage.model")
const SMPS = require("../models/smps.model")
const Cabinets = require("../models/cabinet.model")

const googleClient = new OAuth2Client(process.env.GOOGLE_CLIENT_ID);
const publicCatalogItem = (document) => presentCatalogItem(
  typeof document?.toObject === 'function' ? document.toObject() : document,
  new Date(),
  Math.max(1, Number(process.env.CATALOG_FRESHNESS_HOURS) || 24)
);

const validateObjectIdParam = (req, res, next) => {
  if (!mongoose.isValidObjectId(req.params.id)) return res.status(400).json({ error: 'Invalid resource id' });
  return next();
};

const buildComponentModels = {
  processor: Processor,
  motherboard: Motherboard,
  gpu: GraphicsCard,
  primaryStorage: Storage,
  secondaryStorage: Storage,
  ram: RAM,
  smps: SMPS,
  cabinet: Cabinets,
};

const resolveBuildComponents = async (componentIds) => {
  if (!componentIds || typeof componentIds !== 'object') {
    const error = new Error('Catalog component ids are required');
    error.statusCode = 400;
    throw error;
  }

  const requiredComponentKeys = Object.keys(buildComponentModels).filter((key) => key !== 'secondaryStorage');
  const missing = requiredComponentKeys.filter((key) => !componentIds[key]);
  if (missing.length) {
    const error = new Error(`Missing component ids: ${missing.join(', ')}`);
    error.statusCode = 400;
    throw error;
  }

  const invalid = Object.entries(componentIds).find(([, id]) => id && !mongoose.isValidObjectId(id));
  if (invalid) {
    const error = new Error(`Invalid component id for ${invalid[0]}`);
    error.statusCode = 400;
    throw error;
  }

  const components = Object.fromEntries(await Promise.all(
    Object.entries(buildComponentModels).map(async ([key, Model]) => [key, componentIds[key] ? await Model.findById(componentIds[key]).lean() : null])
  ));
  const notFound = Object.entries(components).filter(([key, value]) => componentIds[key] && !value).map(([key]) => key);
  if (notFound.length) {
    const error = new Error(`Catalog components not found: ${notFound.join(', ')}`);
    error.statusCode = 404;
    throw error;
  }
  return components;
};

const canonicalBuildFields = (components) => ({
  cpu: components.processor.name,
  motherboard: components.motherboard.name,
  gpu: components.gpu.name,
  primaryStorage: components.primaryStorage.name,
  secondaryStorage: components.secondaryStorage?.name || "",
  ram: components.ram.name,
  powerSupply: components.smps.name,
  cabinet: components.cabinet.name,
  image: components.cabinet.image_url || "",
});

const verifyGoogleCredential = async (credential) => {
  if (!process.env.GOOGLE_CLIENT_ID) {
    const error = new Error('Google authentication is not configured');
    error.statusCode = 503;
    throw error;
  }
  if (!credential) {
    const error = new Error('Google credential is required');
    error.statusCode = 400;
    throw error;
  }
  const ticket = await googleClient.verifyIdToken({
    idToken: credential,
    audience: process.env.GOOGLE_CLIENT_ID,
  });
  const payload = ticket.getPayload();
  if (!payload?.email || !payload.email_verified) {
    const error = new Error('Google email is not verified');
    error.statusCode = 401;
    throw error;
  }
  return { email: payload.email.toLowerCase(), fullname: payload.name || payload.email };
};

const serializeUser = (user) => ({
  id: user.id,
  fullname: user.fullname,
  email: user.email,
  isAdmin: isAdminEmail(user.email),
});

const createAuthToken = (user) => {
  if (!process.env.JWT_SECRET) {
    throw new Error('JWT_SECRET is not configured');
  }

  return jwt.sign(
    {
      user: {
        id: user.id,
        email: user.email,
      },
    },
    process.env.JWT_SECRET,
    {
      expiresIn: process.env.JWT_EXPIRES_IN || '7d',
      algorithm: 'HS256',
    }
  );
};

const sendAuthResponse = (res, statusCode, user) => {
  res.status(statusCode).json({
    token: createAuthToken(user),
    user: serializeUser(user),
  });
};

//Google Login
router.post('/googleLogin', async (req, res) => {
  try {
    const { email, fullname } = await verifyGoogleCredential(req.body.credential);
    let user = await User.findOne({ email });

    if (!user) {
      // If user doesn't exist, create a new one
      user = new User({ fullname, email, password: null });
      await user.save();
    }

    sendAuthResponse(res, 200, user);
  } catch (err) {
    console.error('Error during Google login:', err);
    res.status(err.statusCode || 500).json({ error: err.statusCode ? err.message : 'Internal Server Error' });
  }
});


// Login Route
router.post('/login', [
  check('email', 'Please include a valid email').isEmail().normalizeEmail(),
  check('password', 'Password is required').isString().isLength({ min: 1, max: 128 })
], async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }

  const { email, password } = req.body;

  try {
    // Check if user exists
    const user = await User.findOne({ email });
    if (!user) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    // Check if password matches
    if (!user.password) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const isMatch = await bcrypt.compare(password, user.password);
    if (!isMatch) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    sendAuthResponse(res, 200, user);
  } catch (err) {
    console.error('Error during login:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});


//Sign-up Route
router.post('/signup', [
  check('fullname', 'Full name is required').trim().notEmpty().isLength({ max: 100 }),
  check('email', 'Please include a valid email').isEmail().normalizeEmail(),
  check('password', 'Password must be between 8 and 128 characters').isLength({ min: 8, max: 128 }),
], async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }

  try {
    const { fullname, email, password } = req.body;

    // Check if user already exists
    const existingUser = await User.findOne({ email });
    if (existingUser) {
      return res.status(409).json({ error: 'User already exists' });
    }

    // Create a new user
    const newUser = new User({ fullname, email, password });
    await newUser.save();

    sendAuthResponse(res, 201, newUser);
  } catch (err) {
    console.error('Error during sign-up:', err);
    if (err?.code === 11000) return res.status(409).json({ error: 'User already exists' });
    res.status(500).json({ error: 'Internal Server Error' });
  }
});


//Google Signup
router.post('/googleSignup', async (req, res) => {
  try {
    const { email, fullname } = await verifyGoogleCredential(req.body.credential);
    let user = await User.findOne({ email });
    let statusCode = 200;

    if (!user) {
      user = new User({ fullname, email, password: null });
      await user.save();
      statusCode = 201;
    }

    sendAuthResponse(res, statusCode, user);
  } catch (err) {
    console.error('Error during Google sign-up:', err);
    res.status(err.statusCode || 500).json({ error: err.statusCode ? err.message : 'Internal Server Error' });
  }
});

//saves
router.post("/saves", authenticate, async (req, res) => {
  const { componentIds } = req.body;
  
  try {
    const components = await resolveBuildComponents(componentIds);
    const compatibility = evaluateCompatibility(components);
    const analytics = estimatePerformance(components.processor, components.gpu);
    if (compatibility.status !== 'compatible') {
      return res.status(422).json({ error: 'Build does not have complete compatibility evidence', compatibility });
    }
    const newSaves = new saves({
      ...canonicalBuildFields(components),
      email: req.user.email,
      analytics,
      componentIds,
      compatibility,
      compatibilityEngineVersion: compatibility.engine.version,
      verifiedAt: compatibility.engine.evaluatedAt,
    });
    await newSaves.save();
    await recordBuildOutcome({
      eventType: "build_saved", user: req.user, savedBuild: newSaves,
      components, compatibility, analytics,
    }).catch((error) => console.error("Unable to record consented build outcome", error));
    return res.status(201).json({ message: 'Save successful' });
  } catch (err) {
    if (!err.statusCode || err.statusCode >= 500) console.error('Error during saving details', err);
    return res.status(err.statusCode || 500).json({ error: err.statusCode ? err.message : 'Internal Server Error' });
  }
});

router.put("/saves/:id", authenticate, async (req, res) => {
  if (!mongoose.isValidObjectId(req.params.id)) {
    return res.status(400).json({ error: 'Invalid save id' });
  }

  try {
    const components = await resolveBuildComponents(req.body.componentIds);
    const compatibility = evaluateCompatibility(components);
    const analytics = estimatePerformance(components.processor, components.gpu);
    if (compatibility.status !== 'compatible') {
      return res.status(422).json({ error: 'Build does not have complete compatibility evidence', compatibility });
    }
    const update = {
      ...canonicalBuildFields(components),
      componentIds: req.body.componentIds,
      compatibility,
      compatibilityEngineVersion: compatibility.engine.version,
      verifiedAt: compatibility.engine.evaluatedAt,
      analytics,
      updatedAt: new Date(),
    };
    const savedBuild = await saves2.findOneAndUpdate(
      { _id: req.params.id, email: req.user.email },
      { $set: update },
      { new: true, runValidators: true }
    );

    if (!savedBuild) {
      return res.status(404).json({ error: 'Saved build not found' });
    }

    await recordBuildOutcome({
      eventType: "build_updated", user: req.user, savedBuild,
      components, compatibility, analytics,
    }).catch((error) => console.error("Unable to record consented build outcome", error));
    return res.status(200).json(savedBuild);
  } catch (err) {
    if (!err.statusCode || err.statusCode >= 500) console.error('Error updating saved build', err);
    return res.status(err.statusCode || 500).json({ error: err.statusCode ? err.message : 'Internal Server Error' });
  }
});

//saves2
router.get("/saves2", authenticate, async (req, res) => {
  try{
    const saves = await saves2.find({ email: req.user.email });
    res.json(saves)
  }catch (err){
    console.error('Error during getting saves', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

//saves2 Delete
router.delete("/delsaves/:id", authenticate, async (req, res) => {
  try{
    const { id } = req.params
    if (!mongoose.isValidObjectId(id)) {
      return res.status(400).json({ error: 'Invalid save id' });
    }

    const profile = await saves2.findOneAndDelete({ _id: id, email: req.user.email });

    if (!profile){
      return res.status(404).json({ error: 'No profile found' });
    }

    res.status(200).json({ message: "Deleted" })
  }catch (err) {
    console.error('Error during deleting profile', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
})

// GPU
router.get("/GPU", async (req, res) => {
  try {
    const graphicCard = await GraphicsCard.find();
    res.json(graphicCard.map(publicCatalogItem));
  } catch (err) {
    console.error("Error occurred while fetching GPU data:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

router.get("/GPU/:id", validateObjectIdParam, async (req, res) => {
  try {
    const gpuId = req.params.id;
    const graphicCard = await GraphicsCard.findById(gpuId);

    if (!graphicCard) {
      return res.status(404).json({ error: "Graphic Card not found" });
    }

    res.json(publicCatalogItem(graphicCard));
  } catch (err) {
    console.error("Error fetching the GPU:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});


// CPU
router.get("/CPU/:id", validateObjectIdParam, async (req, res) => {
  try {
    const cpuID = req.params.id;
    const processor = await Processor.findById(cpuID);

    if (!processor) {
      return res.status(404).json({ error: "Processor not found" });
    }

    res.json(publicCatalogItem(processor));
  } catch (err) {
    console.error("Error fetching the processor:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

router.get("/CPU", async (req, res) => {
  try {
    const processor = await Processor.find();
    res.json(processor.map(publicCatalogItem));
  } catch (err) {
    console.error("Error occurred while fetching CPU data:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Cabinets
router.get("/cabinet/:id", validateObjectIdParam, async (req, res) => {
  try {
    const cabinetId = req.params.id;
    const cabinet = await Cabinets.findById(cabinetId);

    if (!cabinet) {
      return res.status(404).json({ error: "Cabinet Not Found!" });
    }

    res.json(publicCatalogItem(cabinet));
  } catch (err) {
    console.error("Error fetching the cabinet: ", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

router.get("/cabinet", async (req, res) => {
  try {
    const cabinet = await Cabinets.find();
    res.json(cabinet.map(publicCatalogItem));
  } catch (err) {
    console.error("Error occurred while getting cabinet info:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Storage
router.get("/storage/:id", validateObjectIdParam, async (req, res) => {
  try {
    const storage = await Storage.findById(req.params.id);

    if (!storage) {
      return res.status(404).json({ error: 'Storage not found.' });
    }

    res.json(publicCatalogItem(storage));
  } catch (err) {
    console.error("Error occurred while getting the storage device details", err)
    res.status(500).json({ error: "Internal Server Error" });
  }
});

router.get("/storage", async (req, res) => {
  try {
    const storage = await Storage.find();
    res.json(storage.map(publicCatalogItem));
  } catch (err) {
    console.error("Error occurred in retrieving storage device data.", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// SMPS
router.get("/smps/:id", validateObjectIdParam, async (req, res) => {
  try {
    const smps = await SMPS.findById(req.params.id);

    if (!smps) {
      return res.status(404).json({ error: "SMPS device data not found" });
    }

    res.json(publicCatalogItem(smps));
  } catch (err) {
    console.error("Error occurred while getting SMPS device data.", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

router.get("/smps", async (req, res) => {
  try {
    const smps = await SMPS.find();
    res.json(smps.map(publicCatalogItem));
  } catch (err) {
    console.error("Error occurred in retrieving SMPS device data", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Motherboard
router.get("/motherboard/:id", validateObjectIdParam, async (req, res) => {
  try {
    const motherboard = await Motherboard.findById(req.params.id);

    if (!motherboard) {
      return res.status(404).json({ error: "Motherboard data not found" });
    }

    res.json(publicCatalogItem(motherboard));
  } catch (err) {
    console.error("Error occurred while getting motherboard data.", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

router.get("/motherboard", async (req, res) => {
  try {
    const motherboard = await Motherboard.find();
    res.json(motherboard.map(publicCatalogItem));
  } catch (err) {
    console.error("Error occurred in retrieving MotherBoard devices data", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// RAM
router.get("/ram/:id", validateObjectIdParam, async (req, res) => {
  try {
    const ram = await RAM.findById(req.params.id);

    if (!ram) {
      return res.status(404).json({ error: "RAM data was not found" });
    }

    res.json(publicCatalogItem(ram));
  } catch (err) {
    console.error("Error occurred in retrieving RAM device data", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

router.get("/ram", async (req, res) => {
  try {
    const ram = await RAM.find();
    res.json(ram.map(publicCatalogItem));
  } catch (err) {
    console.error("Error occurred in retrieving RAM device data", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

module.exports = router;
