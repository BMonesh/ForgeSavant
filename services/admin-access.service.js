const normalizeAdminEmail = (value) => {
  const email = String(value || "").trim().toLowerCase();
  const separator = email.lastIndexOf("@");
  if (separator <= 0 || separator === email.length - 1) return email;

  let localPart = email.slice(0, separator);
  let domain = email.slice(separator + 1);
  if (domain === "googlemail.com") domain = "gmail.com";
  if (domain === "gmail.com") {
    localPart = localPart.split("+", 1)[0].replaceAll(".", "");
  }
  return `${localPart}@${domain}`;
};

const adminEmails = () => new Set(
  (process.env.ADMIN_EMAILS || "")
    .split(",")
    .map(normalizeAdminEmail)
    .filter(Boolean)
);

const isAdminEmail = (email) => Boolean(email) && adminEmails().has(normalizeAdminEmail(email));

const requireAdmin = (req, res, next) => {
  if (!isAdminEmail(req.user?.email)) {
    return res.status(403).json({ error: "Administrator access required" });
  }
  return next();
};

module.exports = { isAdminEmail, normalizeAdminEmail, requireAdmin };
