/* eslint-disable react/prop-types */
import { Link } from "react-router-dom";
import { formatPrice, getBuildTotal, stepLabels } from "./buildUtils";
import { useSession } from "../../auth/SessionContext";

const summarySteps = [
  "processor",
  "motherboard",
  "gpu",
  "primaryStorage",
  "secondaryStorage",
  "ram",
  "smps",
  "cabinet",
];

const BuildSummary = ({ selection, estimate, compatibility, compatibilityStatus, saveState, message, sourceSaveId, onSave, onBack }) => {
  const total = getBuildTotal(selection);
  const { isAuthenticated } = useSession();

  return (
    <section className="build-summary" aria-labelledby="build-summary-title">
      <div className="summary-header">
        <p className="ui-kicker">Review build</p>
        <h2 id="build-summary-title">{sourceSaveId ? "Update saved build" : compatibility?.status === "compatible" ? "Server verified" : "Review compatibility"}</h2>
        <p>{compatibilityStatus === "checking" ? "The server is checking the selected catalog records." : `Review the selected parts and evidence before ${sourceSaveId ? "updating" : "saving"} this build.`}</p>
      </div>

      <div className="summary-checks" aria-label="Server compatibility evidence">
        {(compatibility?.checks || []).map((check) => (
          <div key={check.id} className={`summary-check summary-check-${check.status}`}>
            <span>{check.label}</span>
            <strong>{check.status}</strong>
            <p>{check.message}</p>
          </div>
        ))}
        {compatibilityStatus === "error" ? <p className="build-message-inline" role="alert">Server compatibility evidence is temporarily unavailable.</p> : null}
      </div>

      <div className="summary-grid">
        {summarySteps.map((stepId) => {
          const item = selection[stepId];
          return (
            <div key={stepId} className="summary-row">
              <span>{stepLabels[stepId]}</span>
              <strong>{item?.name || (stepId === "secondaryStorage" && selection.skipSecondaryStorage ? "Not included" : "Not selected")}</strong>
              <small>{item ? formatPrice(item.price) : stepId === "secondaryStorage" && selection.skipSecondaryStorage ? "Optional" : "Pending"}</small>
            </div>
          );
        })}
      </div>

      <div className="summary-totals">
        <div>
          <span>Estimated parts total</span>
          <strong>{formatPrice(total)}</strong>
        </div>
        <div>
          <span>PSU target</span>
          <strong>{estimate.psuTarget ? `${estimate.psuTarget}W+` : "Pending"}</strong>
        </div>
        <div>
          <span>Planning capability indicators</span>
          <strong>
            {estimate.performance
              ? `CPU index ${estimate.performance.cpuParallelismIndex} / ${estimate.performance.gpuMemoryGB} GB GPU memory · ${estimate.performance.confidence || "low"} confidence`
              : "Pending"}
          </strong>
          <small>Dimensionless planning index; no benchmark or frame-rate claim.</small>
        </div>
      </div>

      {message ? <p className="build-message-inline" role="alert">{message}</p> : null}

      <div className="summary-actions">
        <button type="button" className="builder-secondary" onClick={onBack}>
          Back to cabinet
        </button>
        {isAuthenticated ? (
          <button
            type="button"
            className="builder-primary"
            onClick={onSave}
            disabled={saveState === "saving" || compatibilityStatus !== "ready" || compatibility?.status !== "compatible"}
          >
            {saveState === "saving" ? "Saving" : sourceSaveId ? "Update build" : "Save build"}
          </button>
        ) : (
          <Link to="/loginAuthentication" state={{ returnTo: "/build" }} className="builder-primary">
            Sign in to save
          </Link>
        )}
      </div>
    </section>
  );
};

export default BuildSummary;
