/* eslint-disable react/prop-types */
import {
  FiBox,
  FiCheck,
  FiCheckSquare,
  FiCpu,
  FiDatabase,
  FiHardDrive,
  FiMonitor,
  FiServer,
  FiZap,
} from "react-icons/fi";

const stepIcons = {
  platform: FiCpu,
  processor: FiCpu,
  motherboard: FiServer,
  gpu: FiMonitor,
  primaryStorage: FiHardDrive,
  secondaryStorage: FiDatabase,
  ram: FiDatabase,
  smps: FiZap,
  cabinet: FiBox,
  review: FiCheckSquare,
};

const compactLabels = {
  platform: "Platform",
  processor: "CPU",
  motherboard: "Board",
  gpu: "GPU",
  primaryStorage: "Primary",
  secondaryStorage: "Secondary",
  ram: "Memory",
  smps: "Power",
  cabinet: "Case",
};

const CompatibilityRail = ({ steps, currentStepId, onStepSelect }) => {
  const completed = steps.filter((step) => step.value !== "Not selected").length;

  return <aside className="compatibility-rail" aria-label="Build sequence">
    <div className="rail-heading">
      <div>
        <p className="ui-kicker">Build sequence</p>
        <h2>Compatibility path</h2>
      </div>
      <span className="rail-progress"><strong>{completed}</strong> of {steps.length} selected</span>
    </div>
    <ol>
      {steps.map((step) => {
        const StepIcon = stepIcons[step.id] || FiBox;
        const hasSelection = step.value !== "Not selected";
        return <li key={step.id}>
          <button
            type="button"
            className={`rail-step rail-${step.status}`}
            onClick={() => onStepSelect(step.id)}
            disabled={!step.canRevisit}
            aria-current={currentStepId === step.id ? "step" : undefined}
            title={hasSelection ? `${step.label}: ${step.value}` : step.meta}
          >
            <span className="rail-index">
              {step.status === "compatible" ? <FiCheck aria-hidden="true" /> : <StepIcon aria-hidden="true" />}
            </span>
            <span className="rail-copy">
              <span className="rail-title">
                <span>{String(step.index).padStart(2, "0")}</span>
                {compactLabels[step.id] || step.label}
              </span>
              <strong>{hasSelection ? step.value : step.meta}</strong>
            </span>
          </button>
        </li>;
      })}
    </ol>
  </aside>;
};

export default CompatibilityRail;
