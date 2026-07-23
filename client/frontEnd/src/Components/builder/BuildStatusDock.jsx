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
}) => {
  const evidenceById = Object.fromEntries((compatibility?.checks || []).map((check) => [check.id, check]));
  const checks = [
    ["Socket", evidenceById["cpu-socket"]?.status === "pass"],
    ["Memory", evidenceById["memory-type"]?.status === "pass"],
    ["Power", evidenceById["power-budget"]?.status === "pass"],
    ["Case fit", evidenceById["case-form-factor"]?.status === "pass"],
  ];

  return (
    <footer className="build-status-dock" aria-label="Build status">
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
      ) : <span className="dock-review-ready">Ready to review</span>}
    </footer>
  );
};

export default BuildStatusDock;
