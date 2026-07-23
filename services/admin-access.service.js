const adminEmails = () => new Set(
  (process.env.ADMIN_EMAILS || "")
    .split(",")
    .map((email) => email.trim().toLowerCase())
    .filter(Boolean)
);

const isAdminEmail = (email) => Boolean(email) && adminEmails().has(String(email).toLowerCase());

const requireAdmin = (req, res, next) => {
  if (!isAdminEmail(req.user?.email)) {
    return res.status(403).json({ error: "Administrator access required" });
  }
  return next();
};

module.exports = { isAdminEmail, requireAdmin };
