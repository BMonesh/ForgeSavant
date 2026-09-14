/* eslint-disable react/prop-types */
import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { FiArrowUpRight, FiBarChart2 } from "react-icons/fi";
import api from "../../services/api";

const normalize = (value) => String(value || "").replace(/[^a-z0-9]/gi, "").toUpperCase();

const panels = [
  { category: "processors", selectionKey: "processor", label: "Processor", noun: "CPU" },
  { category: "gpus", selectionKey: "gpu", label: "Graphics card", noun: "GPU" },
];

/**
 * Where the selected CPU and GPU sit against every catalog product with public
 * benchmark evidence. Scores are Blender Open Data medians matched to the exact
 * manufacturer part number, so a part without a matching record says so rather
 * than borrowing a similar product's score. Nothing here is a frame rate.
 */
const BuildPerformance = ({ selection }) => {
  const [summary, setSummary] = useState(null);
  const [state, setState] = useState("loading");

  useEffect(() => {
    let active = true;
    const load = async () => {
      try {
        const response = await api.get("/api/v1/analytics/benchmarks");
        if (!active) return;
        const data = response?.data?.data;
        if (Array.isArray(data?.records)) {
          setSummary(data);
          setState("ready");
        } else {
          setState("unavailable");
        }
      } catch {
        // The build is still valid without benchmark evidence; say so and move on.
        if (active) setState("unavailable");
      }
    };
    load();
    return () => { active = false; };
  }, []);

  const charts = useMemo(() => panels.map((panel) => {
    const rows = (summary?.records || [])
      .filter((row) => row.category === panel.category && Number.isFinite(Number(row.metricValue)))
      .sort((a, b) => Number(b.metricValue) - Number(a.metricValue));
    const item = selection[panel.selectionKey];
    const partNumber = normalize(item?.identity?.manufacturerPartNumber);
    const selected = partNumber ? rows.find((row) => normalize(row.manufacturerPartNumber) === partNumber) : null;
    const top = rows[0] ? Number(rows[0].metricValue) : 0;
    return { ...panel, rows, item, selected, top };
  }), [selection, summary]);

  if (state === "loading") {
    return <section className="build-performance" aria-busy="true"><p className="performance-state">Loading benchmark evidence…</p></section>;
  }
  if (state === "unavailable") {
    return (
      <section className="build-performance">
        <p className="performance-state">Benchmark evidence is unavailable right now. Compatibility checks above are unaffected.</p>
      </section>
    );
  }

  return (
    <section className="build-performance" aria-labelledby="build-performance-title">
      <div className="performance-heading">
        <div>
          <p className="ui-kicker">Performance evidence</p>
          <h3 id="build-performance-title">How your parts rank in the catalog</h3>
        </div>
        <Link to="/benchmarks" className="performance-link">
          <FiBarChart2 aria-hidden="true" /> All benchmarks
        </Link>
      </div>

      <div className="performance-grid">
        {charts.map((chart) => (
          <article key={chart.category} className="performance-card" aria-label={`${chart.label} benchmark ranking`}>
            <header className="performance-card-head">
              <span>{chart.label}</span>
              <strong>{chart.item?.name || "Not selected"}</strong>
              {chart.selected ? (
                <div className="performance-stats">
                  <div><small>Rank</small><b>#{chart.rows.indexOf(chart.selected) + 1} of {chart.rows.length}</b></div>
                  <div><small>Median score</small><b>{Math.round(chart.selected.metricValue).toLocaleString("en-IN")}</b></div>
                  <div><small>Vs catalog best</small><b>{Math.round((chart.selected.metricValue / chart.top) * 100)}%</b></div>
                  <div><small>Public runs</small><b>{Number(chart.selected.sampleCount || 0).toLocaleString("en-IN")}</b></div>
                </div>
              ) : (
                <p className="performance-missing">
                  No public benchmark matches this exact part number, so no score is shown rather than borrowing a similar product&apos;s.
                </p>
              )}
            </header>

            {chart.rows.length > 0 ? (
              <ol className="performance-bars">
                {chart.rows.map((row) => {
                  const isSelected = row === chart.selected;
                  const width = chart.top > 0 ? Math.max(2, (Number(row.metricValue) / chart.top) * 100) : 0;
                  return (
                    <li
                      key={row.manufacturerPartNumber}
                      className={isSelected ? "is-selected" : ""}
                      aria-current={isSelected ? "true" : undefined}
                    >
                      <span className="bar-label" title={row.catalogName}>{row.catalogName}</span>
                      <span className="bar-track" aria-hidden="true">
                        <span className="bar-fill" style={{ width: `${width}%` }} />
                      </span>
                      <span className="bar-value">{Math.round(row.metricValue).toLocaleString("en-IN")}</span>
                    </li>
                  );
                })}
              </ol>
            ) : null}

            {chart.selected ? (
              <footer className="performance-source">
                <span>{chart.selected.benchmarkName} · {chart.selected.unit}</span>
                <a href={chart.selected.sourceRecordUrl} target="_blank" rel="noreferrer">
                  Source <FiArrowUpRight aria-hidden="true" />
                </a>
              </footer>
            ) : null}
          </article>
        ))}
      </div>

      <ul className="performance-caveats">
        <li>CPU and GPU scores use different scales. Compare within a chart, never across them or added together.</li>
        {(summary?.caveats || []).map((caveat) => <li key={caveat}>{caveat}</li>)}
      </ul>
    </section>
  );
};

export default BuildPerformance;
