/* eslint-disable react/prop-types */
import { FiInfo } from "react-icons/fi";

const bandOrder = ["under-30", "30-60", "60-90", "90-144", "144-plus"];

/**
 * Estimated frame-rate bands for the chosen graphics card.
 *
 * Every state is labelled as an estimate: the bands come from a rendering
 * benchmark calibrated to one GPU generation, not from any game being run.
 * When the model declines a card, the reason is shown rather than hidden, so a
 * missing estimate is never mistaken for a poor one.
 */
const GamingEstimate = ({ gaming }) => {
  if (!gaming) return null;

  return (
    <section className="gaming-estimate" aria-labelledby="gaming-estimate-title">
      <div className="gaming-heading">
        <div>
          <p className="ui-kicker">Estimated gaming performance</p>
          <h3 id="gaming-estimate-title">
            {gaming.status === "estimate" ? `${gaming.preset} preset, ${gaming.workload}` : "No estimate for this card"}
          </h3>
        </div>
        <span className="estimate-badge">Estimated · not measured</span>
      </div>

      {gaming.status === "estimate" ? (
        <>
          <ol className="gaming-tiers">
            {gaming.tiers.map((tier) => {
              const level = bandOrder.indexOf(tier.band);
              return (
                <li key={tier.resolution} className={`gaming-tier level-${level}`}>
                  <span className="tier-resolution">{tier.label}</span>
                  <span className="tier-meter" aria-hidden="true">
                    {bandOrder.slice(0, 4).map((_, index) => (
                      <i key={index} className={index < level ? "on" : ""} />
                    ))}
                  </span>
                  <strong className="tier-band">{tier.bandLabel}</strong>
                </li>
              );
            })}
          </ol>
          <details className="gaming-method">
            <summary><FiInfo aria-hidden="true" /> How this is estimated</summary>
            <p>Based on the {gaming.model.basis}, anchored at {gaming.model.anchor}. Model {gaming.model.version}, {gaming.confidence} confidence.</p>
            <ul>{gaming.assumptions.map((item) => <li key={item}>{item}</li>)}</ul>
          </details>
        </>
      ) : (
        <p className="gaming-unavailable">{gaming.reason}</p>
      )}
    </section>
  );
};

export default GamingEstimate;
