/* eslint-disable react/prop-types */
import { FiCheck, FiChevronRight } from "react-icons/fi";
import { formatPrice, getBuildTotal } from "./buildUtils";

const BuildStatusDock = ({
  selection,
  estimate,
  compatibility,
  compatibilityStatus,
  canContinue,
  isReview,
  onContinue,
  onSave,
  saveState,
  sourceSaveId,
  message,
}) => {
  const evidenceById = Object.fromEntries((compatibility?.checks || []).map((check) => [check.id, check]));
  const checks = [
    ["Socket", evidenceById["cpu-socket"]?.status === "pass"],
    ["Memory", evidenceById["memory-type"]?.status === "pass"],
    ["Power", evidenceById["power-budget"]?.status === "pass"],
    ["Case fit", evidenceById["case-form-factor"]?.status === "pass"],
  ];

  return (
    <footer className="build-status-dock" aria-label="Build status" aria-live="polite">
      <div className="dock-checks">
        <span className="dock-label">Compatibility checks</span>
        <div>
          {checks.map(([label, complete]) => (
            <span key={label} className={complete ? "is-complete" : ""}>
              <FiCheck aria-hidden="true" /> {label}
            </span>
          ))}
        </div>
      </div>
      <div className="dock-metric">
        <span>Estimated power</span>
        <strong>{compatibilityStatus === "checking" ? "Checking" : compatibility?.power?.recommendedPsu ? `${compatibility.power.recommendedPsu}W` : estimate.psuTarget ? `${estimate.psuTarget}W` : "--"}</strong>
      </div>
      <div className="dock-metric">
        <span>Build total</span>
        <strong>{formatPrice(getBuildTotal(selection))}</strong>
      </div>
      {!isReview ? (
        <button
          type="button"
          className="dock-continue"
          onClick={onContinue}
          disabled={!canContinue}
        >
          Continue <FiChevronRight aria-hidden="true" />
        </button>
      ) : (
        // This slot held a status label styled exactly like the Continue
        // button, so on the last step it invited a click and did nothing while
        // the real Save control sat below the fold. It now carries the primary
        // action, and the save message with it, because a dock pinned to the
        // viewport is the one place feedback is guaranteed to be visible.
        <button
          type="button"
          className="dock-continue"
          onClick={onSave}
          disabled={saveState === "saving" || compatibilityStatus !== "ready" || compatibility?.status !== "compatible"}
        >
          {saveState === "saving" ? "Saving" : sourceSaveId ? "Update build" : "Save build"}
        </button>
      )}
      {isReview && message ? (
        <p className="dock-message" role={saveState === "error" ? "alert" : "status"}>{message}</p>
      ) : null}
    </footer>
  );
};

export default BuildStatusDock;
