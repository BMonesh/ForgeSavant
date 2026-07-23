const jwt = require("jsonwebtoken");
const User = require("../models/user.model");
const { isAdminEmail } = require("../services/admin-access.service");

const authenticate = async (req, res, next) => {
  const authHeader = req.get("Authorization") || "";
  const [scheme, token] = authHeader.split(" ");

  if (scheme !== "Bearer" || !token) {
    return res.status(401).json({ error: "Authentication required" });
  }

  if (!process.env.JWT_SECRET) {
    return res.status(500).json({ error: "JWT secret is not configured" });
  }

  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET, {
      algorithms: ["HS256"],
    });
    const userId = decoded.user?.id;

    if (!userId) {
      return res.status(401).json({ error: "Invalid token" });
    }

    const user = await User.findById(userId).select("-password");
    if (!user) {
      return res.status(401).json({ error: "Invalid token" });
    }

    req.user = {
      id: user.id,
      fullname: user.fullname,
      email: user.email,
      isAdmin: isAdminEmail(user.email),
    };
    next();
  } catch (err) {
    return res.status(401).json({ error: "Invalid or expired token" });
  }
};

module.exports = { authenticate };
