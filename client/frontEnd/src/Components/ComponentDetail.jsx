import { useEffect, useMemo, useState } from "react";
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
  : "Not recorded";

const formatDate = (value) => value
  ? new Date(value).toLocaleString("en-IN", { dateStyle: "medium", timeStyle: "short" })
  : "Not recorded";

const ComponentDetail = () => {
  const { category, id } = useParams();
  const [component, setComponent] = useState(null);
  const [state, setState] = useState("loading");

  useEffect(() => {
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

  const specifications = useMemo(() => Object.entries(component?.specifications || {}), [component]);

  if (state === "loading") return <div className="component-detail-state"><FiRefreshCw aria-hidden="true" /> Loading catalog evidence…</div>;
  if (state === "error" || !component) return <div className="component-detail-state"><strong>Component evidence is unavailable.</strong><Link to="/build">Return to builder</Link></div>;

  return (
    <div className="component-detail-page">
      <header className="component-detail-hero">
        <div>
          <Link className="component-back" to="/build"><FiArrowLeft aria-hidden="true" /> Back to builder</Link>
          <p className="ui-kicker">Catalog evidence / {label(category)}</p>
          <h1>{component.name}</h1>
          <p>{component.manufacturer} · Canonical catalog record</p>
        </div>
        <div className={`pricing-proof ${component.pricing.status}`}>
          <span>{component.pricing.status === "live" ? "Live observed price" : component.pricing.status === "stale" ? "Stale observed price" : "Planning price"}</span>
          <strong>{formatPrice(component.price)}</strong>
          <small>{component.pricing.status === "sample" ? "Sample catalog value · No retailer observation" : `${component.pricing.source} · ${formatDate(component.pricing.observedAt)}`}</small>
          {component.pricing.sourceUrl ? <a href={component.pricing.sourceUrl} target="_blank" rel="noreferrer">Open source <FiExternalLink aria-hidden="true" /></a> : null}
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
            {specifications.map(([key, value]) => <div key={key}><dt>{label(key)}</dt><dd>{Array.isArray(value) ? value.join(", ") : String(value)}</dd></div>)}
          </dl>
        </section>
      </div>

      <section className="component-history">
        <div className="component-section-heading"><div><p className="ui-kicker">Price history</p><h2>Recorded pricing trail.</h2><p>Authorized observations and clearly identified sample baselines.</p></div><span>{component.priceHistory?.length || 0} records</span></div>
        {component.priceHistory?.length ? (
          <div className="component-table-wrap"><table><thead><tr><th>Observed</th><th>Source</th><th>Availability</th><th>Price</th></tr></thead><tbody>
            {component.priceHistory.map((entry, index) => {
              const isObserved = Boolean(entry.observedAt && entry.sourceUrl && entry.importChecksum);
              return <tr key={`${entry.source}-${entry.observedAt}-${index}`}><td>{formatDate(entry.observedAt || entry.recordedAt)}</td><td>{isObserved ? <a href={entry.sourceUrl} target="_blank" rel="noreferrer">{entry.source} <FiExternalLink aria-hidden="true" /></a> : "Sample baseline"}</td><td>{isObserved ? label(entry.availability) : "Not observed"}</td><td>{formatPrice(entry.price)}</td></tr>;
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
        <div className="component-section-heading"><div><p className="ui-kicker">Retailer mappings</p><h2>Persistent product relationships.</h2></div><span>{component.retailerMappings?.length || 0} sources</span></div>
        {component.retailerMappings?.length ? <div className="mapping-list">{component.retailerMappings.map((mapping) => <article key={`${mapping.source}-${mapping.sourceItemId}`}><strong>{mapping.source}</strong><span>{mapping.sourceTitle}</span><code>{mapping.sourceItemId}</code><small>{label(mapping.matchMethod)} · last seen {formatDate(mapping.lastSeenAt)}</small></article>)}</div> : <p className="component-empty">No retailer products are mapped to this catalog record yet.</p>}
      </section>
    </div>
  );
};

export default ComponentDetail;
