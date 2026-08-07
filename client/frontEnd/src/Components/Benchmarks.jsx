import { useEffect, useMemo, useState } from "react";
import { FiArrowUpRight, FiBarChart2, FiRefreshCw } from "react-icons/fi";
import api from "../services/api";
import "../Styles/Benchmarks.css";

const labels = { processors: "Processors", gpus: "Graphics cards" };

const Benchmarks = () => {
  const [summary, setSummary] = useState(null);
  const [category, setCategory] = useState("gpus");
  const [state, setState] = useState("loading");
  const [message, setMessage] = useState("");

  const load = async () => {
    setState("loading");
    setMessage("");
    try {
      const response = await api.get("/api/v1/analytics/benchmarks");
      setSummary(response.data.data);
      setState("ready");
    } catch (error) {
      setState("error");
      setMessage(error.response?.data?.error || "Benchmark evidence could not be loaded");
    }
  };

  useEffect(() => { load(); }, []);
  const records = useMemo(
    () => (summary?.records || []).filter((row) => row.category === category),
    [category, summary],
  );

  if (state === "loading" && !summary) return <div className="benchmark-state" role="status">Loading benchmark evidence…</div>;
  if (state === "error") return <div className="benchmark-state error" role="alert"><strong>Benchmark data unavailable</strong><span>{message}</span><button type="button" onClick={load}><FiRefreshCw aria-hidden="true" /> Retry</button></div>;

  return (
    <div className="benchmark-page">
      <header className="benchmark-hero">
        <div><p className="ui-kicker">Performance evidence / Blender 5.1.1</p><h1>Compare one workload. See the limits.</h1><p>Current public aggregate medians for exact catalog identities. Rankings stay within a component category and never use retailer commission.</p></div>
        <div className="benchmark-proof"><FiBarChart2 aria-hidden="true" /><strong>{summary.counts.total}</strong><span>current aggregates</span><small>Updated {new Date(summary.generatedAt).toLocaleString()}</small></div>
      </header>

      <section className="benchmark-controls" aria-label="Benchmark category">
        {Object.entries(labels).map(([key, label]) => <button key={key} type="button" className={category === key ? "active" : ""} aria-pressed={category === key} onClick={() => setCategory(key)}>{label}<span>{summary.counts[key]}</span></button>)}
      </section>

      <section className="benchmark-table-wrap" aria-labelledby="benchmark-table-title">
        <div className="benchmark-heading"><div><span>Current evidence</span><h2 id="benchmark-table-title">{labels[category]}</h2></div><small>Higher Blender Benchmark points rank first</small></div>
        <div className="benchmark-table-scroll">
          <table className="benchmark-table">
            <thead><tr><th scope="col">Rank</th><th scope="col">Exact catalog product</th><th scope="col">Median score</th><th scope="col">Public samples</th><th scope="col">Collected</th><th scope="col">Evidence</th></tr></thead>
            <tbody>{records.map((row) => <tr key={`${row.category}:${row.manufacturerPartNumber}`}><td><strong>#{row.categoryRank}</strong></td><td><strong>{row.catalogName}</strong><span>{row.manufacturerPartNumber}</span></td><td><strong>{Math.round(row.metricValue).toLocaleString()}</strong><span>{row.unit}</span></td><td>{row.sampleCount?.toLocaleString() || "—"}</td><td>{new Date(row.observedAt).toLocaleDateString()}</td><td><a href={row.sourceRecordUrl} target="_blank" rel="noreferrer">Open source <FiArrowUpRight aria-hidden="true" /></a></td></tr>)}</tbody>
          </table>
        </div>
      </section>

      <section className="benchmark-guardrails" aria-labelledby="benchmark-guardrails-title"><h2 id="benchmark-guardrails-title">Interpretation guardrails</h2><ul>{summary.caveats.map((item) => <li key={item}>{item}</li>)}</ul></section>
    </div>
  );
};

export default Benchmarks;
