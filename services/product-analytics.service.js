const crypto = require("node:crypto");
const AnalyticsEvent = require("../models/analyticsEvent.model");

const pseudonymSecret = () => process.env.ANALYTICS_PSEUDONYM_SECRET || process.env.JWT_SECRET;

const pseudonym = (namespace, value) => {
  const secret = pseudonymSecret();
  if (!secret) throw new Error("Analytics pseudonym secret is not configured");
  return crypto.createHmac("sha256", secret).update(`${namespace}:${String(value)}`).digest("hex");
};

const componentTotal = (components) => Object.values(components)
  .filter(Boolean)
  .reduce((total, component) => total + (Number(component.price) || 0), 0);

const recordBuildOutcome = async ({ eventType, user, savedBuild, components, compatibility, analytics }) => {
  if (!user?.analyticsConsent) return null;
  return AnalyticsEvent.create({
    schemaVersion: "1.0",
    eventType,
    subjectHash: pseudonym("user", user.id),
    buildHash: pseudonym("build", savedBuild.id || savedBuild._id),
    componentIds: savedBuild.componentIds,
    buildTotal: componentTotal(components),
    currency: "INR",
    compatibilityStatus: compatibility.status,
    compatibilityEngineVersion: compatibility.engine.version,
    analyticsModelVersion: analytics.model.version,
    occurredAt: new Date(),
  });
};

const deleteSubjectEvents = async (userId) => AnalyticsEvent.deleteMany({
  subjectHash: pseudonym("user", userId),
});

module.exports = { pseudonym, componentTotal, recordBuildOutcome, deleteSubjectEvents };
