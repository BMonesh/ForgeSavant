import { useCallback, useEffect, useMemo, useState } from "react";
import { FiArrowLeft, FiExternalLink, FiRefreshCw } from "react-icons/fi";
import { Link, useParams } from "react-router-dom";
import api from "../services/api";
import "../Styles/ComponentDetail.css";

const label = (value) => String(value || "")
  .replace(/_/g, " ")
  .replace(/([a-z])([A-Z])/g, "$1 $2")
  .replace(/^./, (character) => character.toUpperCase());

const formatPrice = (value) => Number.isFinite(Number(value))
  ? `₹${Number(value).toLocaleString("en-IN")}`
  : "Unavailable";

const formatDate = (value) => value
  ? new Date(value).toLocaleString("en-IN", { dateStyle: "medium", timeStyle: "short" })
  : "Unavailable";

const priceEvidence = (component) => {
  const pricing = component?.pricing || {};
  if (pricing.status === "live") return { key: "live", label: "Live observation" };
  if (pricing.status === "stale") return { key: "stale", label: "Stale observation" };
  if (Number.isFinite(Number(component?.price))) return { key: "planning", label: "Planning value" };
  return { key: "unavailable", label: "Price unavailable" };
};

const ComponentDetail = () => {
  const { category, id } = useParams();
  const [component, setComponent] = useState(null);
  const [state, setState] = useState("loading");

  const loadComponent = useCallback(() => {
    let active = true;
    setState("loading");
    api.get(`/api/v1/catalog/${category}/${id}`)
      .then((response) => {
        if (!active) return;
        setComponent(response.data.data);
        setState("ready");
      })
      .catch(() => { if (active) setState("error"); });
    return () => { active = false; };
  }, [category, id]);

  useEffect(() => loadComponent(), [loadComponent]);

  const specifications = useMemo(() => Object.entries(component?.specifications || {}), [component]);
  const affiliateDestinations = useMemo(
    () => component?.retailerMappings?.filter((mapping) => mapping.relationshipType === "affiliate_link" && mapping.sourceUrl) || [],
    [component]
  );
  const retailerMappings = useMemo(
    () => component?.retailerMappings?.filter((mapping) => mapping.relationshipType !== "affiliate_link") || [],
    [component]
  );
  const pricing = component?.pricing || {};
  const pricingState = priceEvidence(component);

  if (state === "loading") return (
    <section className="component-detail-state component-detail-loading" aria-busy="true" aria-live="polite">
      <span className="sr-only">Loading catalog evidence</span>
      <div className="detail-skeleton-heading" aria-hidden="true" />
      <div className="detail-skeleton-rule" aria-hidden="true" />
      <div className="detail-skeleton-row" aria-hidden="true" />
      <div className="detail-skeleton-row" aria-hidden="true" />
      <div className="detail-skeleton-row short" aria-hidden="true" />
    </section>
  );
  if (state === "error" || !component) return (
    <section className="component-detail-state component-detail-error" role="alert">
      <p className="ui-kicker">Catalog / request failed</p>
      <h1>Component evidence is unavailable</h1>
      <p>The catalog record could not be retrieved. No planning or retailer data is being shown.</p>
      <div className="component-state-actions">
        <button type="button" onClick={loadComponent}><FiRefreshCw aria-hidden="true" /> Retry</button>
        <Link to="/build"><FiArrowLeft aria-hidden="true" /> Return to builder</Link>
      </div>
    </section>
  );

  return (
    <div className="component-detail-page">
      <header className="component-detail-hero">
        <div>
          <Link className="component-back" to="/build"><FiArrowLeft aria-hidden="true" /> Back to builder</Link>
          <p className="ui-kicker">Catalog evidence / {label(category)}</p>
          <h1>{component.name}</h1>
          <p>{component.manufacturer} · Canonical catalog record</p>
        </div>
        <div className={`pricing-proof ${pricingState.key}`} aria-label={`Price evidence: ${pricingState.label}`}>
          <span className={`evidence-label ${pricingState.key}`}>{pricingState.label}</span>
          <strong>{formatPrice(component.price)}</strong>
          <small>{pricing.status === "sample" ? "Planning catalog value · No retailer observation" : pricingState.key === "unavailable" ? "No authorized retailer observation or planning value is recorded." : `${pricing.source || "Source unavailable"} · ${formatDate(pricing.observedAt)}`}</small>
          {pricing.sourceUrl ? <a href={pricing.sourceUrl} target="_blank" rel="noreferrer">Open source <FiExternalLink aria-hidden="true" /></a> : null}
        </div>
      </header>

      <div className="component-detail-grid">
        <section className="component-evidence-card">
          <p className="ui-kicker">Product identity</p>
          <h2>One component, across sources.</h2>
          <dl className="component-identity">
            <div><dt>Canonical key</dt><dd><code>{component.identity?.canonicalKey || "Not assigned"}</code></dd></div>
            <div><dt>Manufacturer part number</dt><dd>{component.identity?.manufacturerPartNumberSourceUrl ? <a href={component.identity.manufacturerPartNumberSourceUrl} target="_blank" rel="noreferrer">{component.identity.manufacturerPartNumber} <FiExternalLink aria-hidden="true" /></a> : component.identity?.manufacturerPartNumber || "Not supplied"}</dd></div>
            <div><dt>Lifecycle</dt><dd>{label(component.identity?.lifecycleStatus || "unknown")}</dd></div>
            <div><dt>Known aliases</dt><dd>{component.identity?.aliases?.length ? component.identity.aliases.join(", ") : "None"}</dd></div>
          </dl>
        </section>

        <section className="component-evidence-card">
          <p className="ui-kicker">Structured specifications</p>
          <h2>Facts used by compatibility rules.</h2>
          <dl className="component-specs">
            {specifications.length ? specifications.map(([key, value]) => <div key={key}><dt>{label(key)}</dt><dd>{Array.isArray(value) ? value.join(", ") : String(value)}</dd></div>) : <div><dt>Specification evidence</dt><dd><span className="evidence-label missing">Unavailable</span> No structured fields are recorded.</dd></div>}
          </dl>
        </section>
      </div>

      <section className="component-history">
        <div className="component-section-heading"><div><p className="ui-kicker">Price history</p><h2>Recorded pricing trail.</h2><p>Authorized observations and clearly identified sample baselines.</p></div><span>{component.priceHistory?.length || 0} records</span></div>
        {component.priceHistory?.length ? (
          <div className="component-table-wrap"><table><thead><tr><th scope="col">Observed</th><th scope="col">Source</th><th scope="col">Availability</th><th scope="col">Price</th></tr></thead><tbody>
            {component.priceHistory.map((entry, index) => {
              const isObserved = Boolean(entry.observedAt && entry.sourceUrl && entry.importChecksum);
              return <tr key={`${entry.source}-${entry.observedAt}-${index}`}><td>{formatDate(entry.observedAt || entry.recordedAt)}</td><td>{isObserved ? <a href={entry.sourceUrl} target="_blank" rel="noreferrer">{entry.source} <FiExternalLink aria-hidden="true" /></a> : <span className="evidence-label planning">Sample baseline</span>}</td><td>{isObserved ? label(entry.availability) : "Not observed"}</td><td>{formatPrice(entry.price)}</td></tr>;
            })}
          </tbody></table></div>
        ) : <p className="component-empty">No historical observations have been recorded yet.</p>}
      </section>

      <section className="component-history">
        <div className="component-section-heading"><div><p className="ui-kicker">Product content evidence</p><h2>Source observations, kept separate.</h2><p>External content can support identity and inspection, but never silently replaces compatibility rules.</p></div><span>{component.productContentEvidence?.length || 0} observations</span></div>
        {component.productContentEvidence?.length ? (
          <div className="content-evidence-list">
            {component.productContentEvidence.map((entry) => (
              <article key={entry.observationId}>
                <div><strong>{label(entry.source)}</strong><span>{entry.sourceTier ? `${label(entry.sourceTier)} tier` : "Source tier unknown"}</span></div>
                <div><small>Observed</small><strong>{formatDate(entry.observedAt)}</strong></div>
                <div><small>GTINs</small><strong>{entry.gtins?.length ? entry.gtins.join(", ") : "Not supplied"}</strong></div>
                <div><small>Structured fields</small><strong>{Object.keys(entry.specifications || {}).length}</strong></div>
                {entry.sourceRecordUrl ? <a href={entry.sourceRecordUrl} target="_blank" rel="noreferrer">Inspect source record <FiExternalLink aria-hidden="true" /></a> : null}
              </article>
            ))}
          </div>
        ) : <p className="component-empty">No reviewed product-content evidence has been promoted yet.</p>}
      </section>

      <section className="component-history">
        <div className="component-section-heading">
          <div>
            <p className="ui-kicker">Retail destinations</p>
            <h2>Reviewed places to continue.</h2>
            <p>Paid links do not establish price or availability. Confirm both on the retailer&apos;s website.</p>
          </div>
          <span>{affiliateDestinations.length} paid link{affiliateDestinations.length === 1 ? "" : "s"}</span>
        </div>
        {affiliateDestinations.length ? (
          <>
            <div className="affiliate-destination-list">
              {affiliateDestinations.map((mapping) => (
                <article key={`${mapping.source}-${mapping.sourceItemId}`}>
                  <div><strong>{mapping.sourceTitle || component.name}</strong><small>ASIN {mapping.sourceItemId}</small></div>
                  <a href={mapping.sourceUrl} target="_blank" rel="noreferrer sponsored nofollow">
                    View on Amazon.in <FiExternalLink aria-hidden="true" />
                  </a>
                </article>
              ))}
            </div>
            <p className="affiliate-inline-disclosure">As an Amazon Associate I earn from qualifying purchases. <Link to="/affiliate-disclosure">Learn how affiliate links work.</Link></p>
          </>
        ) : <p className="component-empty">No reviewed retailer destination is mapped to this component yet.</p>}
      </section>

      <section className="component-history">
        <div className="component-section-heading"><div><p className="ui-kicker">Retailer mappings</p><h2>Persistent product relationships.</h2></div><span>{retailerMappings.length} sources</span></div>
        {retailerMappings.length ? <div className="mapping-list">{retailerMappings.map((mapping) => <article key={`${mapping.source}-${mapping.sourceItemId}`}><strong>{mapping.source}</strong><span>{mapping.sourceTitle}</span><code>{mapping.sourceItemId}</code><small>{label(mapping.matchMethod)} · last seen {formatDate(mapping.lastSeenAt)}</small></article>)}</div> : <p className="component-empty">No price-feed retailer products are mapped to this catalog record yet.</p>}
      </section>
    </div>
  );
};

export default ComponentDetail;
