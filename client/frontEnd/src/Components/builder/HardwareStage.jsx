/* eslint-disable react/prop-types */
import { FiCheck, FiCpu } from "react-icons/fi";
import { Link } from "react-router-dom";
import { formatPrice, getItemUtilityFacts, getRuleEvidence } from "./buildUtils";

const HardwareStage = ({ stepId, title, selection, selectedPart, image }) => {
  const facts = getItemUtilityFacts(selectedPart, stepId).slice(0, 3);
  const isReview = stepId === "review";
  const categoryByStep = { processor: "processors", motherboard: "motherboards", gpu: "gpus", ram: "ram", smps: "powerSupplies", cabinet: "cabinets", primaryStorage: "storage", secondaryStorage: "storage" };
  const detailCategory = categoryByStep[stepId];

  return <section className="hardware-stage" aria-labelledby="hardware-stage-title">
    <div className="stage-heading">
      <p className="ui-kicker">Selection preview</p>
      <h1 id="hardware-stage-title">{selectedPart?.name || title}</h1>
      <span>{isReview ? "Build complete" : selectedPart ? formatPrice(selectedPart.price) : "Awaiting selection"}</span>
    </div>

    <div className="stage-visual">
      <div className={`stage-visual-frame ${selectedPart ? "has-selection" : ""}`}>
        <span className="stage-axis stage-axis-horizontal" aria-hidden="true" />
        <span className="stage-axis stage-axis-vertical" aria-hidden="true" />
        <img src={image} alt={selectedPart ? selectedPart.name : "PC hardware workbench"} />
      </div>
      <div className="stage-callout">
        <FiCpu aria-hidden="true" />
        <span>{isReview ? "Review ready" : selectedPart ? "Selected" : stepId === "platform" ? "Platform first" : "Rule checked"}</span>
      </div>
      {facts.length ? <div className="stage-facts">{facts.map((fact) => <span key={fact}>{fact}</span>)}</div> : null}
    </div>

    <div className="stage-evidence">
      <span className="stage-check"><FiCheck aria-hidden="true" /></span>
      <div>
        <p className="ui-kicker">Compatibility evidence</p>
        <strong>{isReview ? "All parts compatible" : selectedPart ? "Selection is compatible" : "Ready for selection"}</strong>
        <p>{isReview ? "Review the configuration or update the saved record." : getRuleEvidence(selection, stepId)}</p>
        {selectedPart && detailCategory ? <Link className="stage-evidence-link" to={`/components/${detailCategory}/${selectedPart._id}`}>View price and source evidence</Link> : null}
      </div>
    </div>
  </section>;
};

export default HardwareStage;
