import { useCallback, useEffect, useMemo, useState } from "react";
import { FiActivity, FiAlertTriangle, FiCheckCircle, FiDatabase, FiRefreshCw } from "react-icons/fi";
import api from "../services/api";
import "../Styles/AdminDataQuality.css";

const categoryLabels = {
  processors: "Processors",
  gpus: "Graphics cards",
  motherboards: "Motherboards",
  ram: "Memory",
  storage: "Storage",
  power_supplies: "Power supplies",
  cabinets: "Cases",
};

const percentage = (value) => value == null ? "—" : `${(value * 100).toFixed(value === 1 || value === 0 ? 0 : 1)}%`;
const boundedPercentage = (numerator, denominator) => denominator > 0 ? Math.min(100, (numerator / denominator) * 100) : 0;

const AdminDataQuality = () => {
  const [summary, setSummary] = useState(null);
  const [state, setState] = useState("loading");
  const [message, setMessage] = useState("");

  const loadSummary = useCallback(async () => {
    setState("loading");
    setMessage("");
    try {
      const response = await api.get("/api/v1/admin/analytics/data-quality");
      setSummary(response.data.data);
      setState("ready");
    } catch (error) {
      setSummary(null);
      setState("error");
      setMessage(error.response?.data?.error || "The analytical summary could not be loaded");
    }
  }, []);

  useEffect(() => { loadSummary(); }, [loadSummary]);

  const categories = useMemo(() => Object.entries(summary?.categories || {}), [summary]);

  if (state === "loading" && !summary) {
    return <div className="quality-state" role="status"><FiActivity aria-hidden="true" /><strong>Loading data health…</strong></div>;
  }
  if (state === "error") {
    return <div className="quality-state error" role="alert"><FiAlertTriangle aria-hidden="true" /><strong>Analytics unavailable</strong><span>{message}</span><button type="button" onClick={loadSummary}>Try again</button></div>;
  }

  const { catalog, pipeline, quality, freshness } = summary;
  const statusLabel = summary.status === "healthy" ? "Pipeline healthy" : summary.status === "stale" ? "Pipeline stale" : "Review required";

  return (
    <div className="quality-page">
      <header className="quality-hero">
        <div>
          <p className="ui-kicker">Data operations / quality</p>
          <h1>Know what the catalog can prove.</h1>
          <p>Source-backed coverage, ingestion reliability, and evidence gaps for every verified component category.</p>
        </div>
        <div className={`quality-status ${summary.status}`}>
          {summary.status === "healthy" ? <FiCheckCircle aria-hidden="true" /> : <FiAlertTriangle aria-hidden="true" />}
          <div><strong>{statusLabel}</strong><span>Generated {new Date(summary.generatedAt).toLocaleString()}</span></div>
          <button type="button" onClick={loadSummary} disabled={state === "loading"}><FiRefreshCw aria-hidden="true" /> Refresh</button>
        </div>
      </header>

      <section className="quality-kpis" aria-label="Data quality summary">
        <article><span>Verified catalog</span><strong>{catalog.verifiedProducts}</strong><small>Curated identities</small></article>
        <article><span>Observed coverage</span><strong>{percentage(catalog.coverageRate)}</strong><small>{catalog.observedProducts} products with accepted evidence</small></article>
        <article><span>Validation pass</span><strong>{percentage(pipeline.validationPassRate)}</strong><small>{pipeline.accepted} new, {pipeline.duplicates} already known</small></article>
        <article><span>Quarantine rate</span><strong>{percentage(quality.quarantineRate)}</strong><small>{pipeline.quarantined} records require review</small></article>
        <article><span>Freshness</span><strong>{Math.round(freshness.ageHours)}h</strong><small>Threshold {freshness.thresholdHours}h</small></article>
      </section>

      <div className="quality-grid">
        <section className="quality-panel quality-coverage" aria-labelledby="coverage-title">
          <div className="quality-panel-heading"><div><span>01</span><h2 id="coverage-title">Catalog evidence coverage</h2></div><small>Accepted Open Icecat products / verified identities</small></div>
          <div className="quality-bars">
            {categories.map(([key, value]) => (
              <div className="quality-bar-row" key={key}>
                <div><strong>{categoryLabels[key] || key}</strong><span>{value.sourceCoverage} / {value.verifiedCatalogProducts}</span></div>
                <div className="quality-track" aria-label={`${categoryLabels[key] || key}: ${value.sourceCoverage} of ${value.verifiedCatalogProducts}`}>
                  <span style={{ width: `${boundedPercentage(value.sourceCoverage, value.verifiedCatalogProducts)}%` }} />
                </div>
              </div>
            ))}
          </div>
        </section>

        <section className="quality-panel" aria-labelledby="completeness-title">
          <div className="quality-panel-heading"><div><span>02</span><h2 id="completeness-title">Accepted observation quality</h2></div></div>
          <dl className="quality-rates">
            <div><dt>Identity completeness</dt><dd>{percentage(quality.identityCompletenessRate)}</dd><span style={{ width: `${(quality.identityCompletenessRate || 0) * 100}%` }} /></div>
            <div><dt>GTIN coverage</dt><dd>{percentage(quality.gtinCoverageRate)}</dd><span style={{ width: `${(quality.gtinCoverageRate || 0) * 100}%` }} /></div>
            <div><dt>Image coverage</dt><dd>{percentage(quality.imageCoverageRate)}</dd><span style={{ width: `${(quality.imageCoverageRate || 0) * 100}%` }} /></div>
          </dl>
          <div className="quality-source-mix">
            <h3>Open Icecat disposition</h3>
            <div className="quality-stack" aria-label="Open Icecat source disposition">
              <span className="available" style={{ flex: catalog.openIcecatAvailable }} />
              <span className="restricted" style={{ flex: catalog.openIcecatRestricted }} />
              <span className="unavailable" style={{ flex: catalog.openIcecatUnavailable }} />
            </div>
            <ul>
              <li><i className="available" />Available <strong>{catalog.openIcecatAvailable}</strong></li>
              <li><i className="restricted" />Paid-tier restricted <strong>{catalog.openIcecatRestricted}</strong></li>
              <li><i className="unavailable" />Not present <strong>{catalog.openIcecatUnavailable}</strong></li>
            </ul>
          </div>
        </section>

        <section className="quality-panel quality-runs" aria-labelledby="runs-title">
          <div className="quality-panel-heading"><div><span>03</span><h2 id="runs-title">Pipeline operations</h2></div><FiDatabase aria-hidden="true" /></div>
          <dl>
            <div><dt>Completed runs</dt><dd>{pipeline.runs}</dd></div>
            <div><dt>Received</dt><dd>{pipeline.received}</dd></div>
            <div><dt>Accepted</dt><dd>{pipeline.accepted}</dd></div>
            <div><dt>Duplicates</dt><dd>{pipeline.duplicates}</dd></div>
            <div><dt>Quarantined</dt><dd>{pipeline.quarantined}</dd></div>
          </dl>
          {summary.operational ? (
            <p><strong>Last orchestrated job</strong>{summary.operational.status} · {summary.operational.stages.length} stages</p>
          ) : null}
          <p><strong>Grain</strong>{summary.grain}</p>
        </section>

        <section className="quality-panel quality-notes" aria-labelledby="notes-title">
          <div className="quality-panel-heading"><div><span>04</span><h2 id="notes-title">Interpretation guardrails</h2></div></div>
          <ul>{summary.caveats.map((caveat) => <li key={caveat}>{caveat}</li>)}</ul>
          <details><summary>Metric definitions</summary><dl>{Object.entries(summary.definitions).map(([key, value]) => <div key={key}><dt>{key.replace(/([A-Z])/g, " $1")}</dt><dd>{value}</dd></div>)}</dl></details>
        </section>

        {summary.modelReadiness ? (
          <section className="quality-panel quality-model" aria-labelledby="model-title">
            <div className="quality-panel-heading"><div><span>05</span><h2 id="model-title">Model readiness</h2></div><small>Evidence gates, not aspirational scores</small></div>
            <div className="quality-model-grid">
              {summary.modelReadiness.uses.map((item) => (
                <article key={item.use}>
                  <span className={`quality-gate ${item.status}`}>{item.status}</span>
                  <h3>{item.use}</h3>
                  <p>{item.reason}</p>
                </article>
              ))}
            </div>
          </section>
        ) : null}
      </div>
    </div>
  );
};

export default AdminDataQuality;
