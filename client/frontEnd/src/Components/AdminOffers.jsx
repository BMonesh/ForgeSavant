import { useEffect, useMemo, useState } from "react";
import { FiAlertTriangle, FiCheck, FiDatabase, FiDownload, FiUploadCloud } from "react-icons/fi";
import api from "../services/api";
import { parseOfferFile } from "./admin/offerFile";
import "../Styles/AdminOffers.css";

const statusCopy = {
  accepted: "Ready",
  ambiguous: "Review",
  unmatched: "No match",
  rejected: "Rejected",
};

const AdminOffers = () => {
  const [source, setSource] = useState("");
  const [offers, setOffers] = useState([]);
  const [fileName, setFileName] = useState("");
  const [preview, setPreview] = useState(null);
  const [resolutions, setResolutions] = useState({});
  const [resolutionsDirty, setResolutionsDirty] = useState(false);
  const [history, setHistory] = useState([]);
  const [state, setState] = useState("idle");
  const [message, setMessage] = useState("");

  const loadHistory = async () => {
    try {
      const response = await api.get("/api/v1/admin/offers/history");
      setHistory(response.data.data || []);
    } catch {
      setHistory([]);
    }
  };

  useEffect(() => { loadHistory(); }, []);

  const handleFile = async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    setPreview(null);
    setResolutions({});
    setResolutionsDirty(false);
    setMessage("");
    try {
      const parsed = await parseOfferFile(file);
      if (parsed.offers.length > 500) throw new Error("A feed can contain at most 500 records");
      setOffers(parsed.offers);
      if (parsed.source) setSource(parsed.source);
      setFileName(file.name);
    } catch (error) {
      setOffers([]);
      setFileName("");
      setMessage(error.message || "Unable to read this feed");
    }
  };

  const previewFeed = async (event) => {
    event?.preventDefault();
    setState("previewing");
    setMessage("");
    try {
      const response = await api.post("/api/v1/admin/offers/preview", {
        source,
        offers,
        resolutions: Object.entries(resolutions).map(([index, componentId]) => ({ index: Number(index), componentId })),
      });
      setPreview(response.data.data);
      setResolutionsDirty(false);
      setState("ready");
    } catch (error) {
      setPreview(null);
      setState("error");
      setMessage(error.response?.data?.error || "The feed could not be reviewed");
    }
  };

  const applyFeed = async () => {
    setState("applying");
    setMessage("");
    try {
      const response = await api.post("/api/v1/admin/offers/apply", {
        source,
        offers,
        resolutions: Object.entries(resolutions).map(([index, componentId]) => ({ index: Number(index), componentId })),
        previewToken: preview.previewToken,
      });
      const applied = response.data.data?.counts?.applied || 0;
      setState("applied");
      setMessage(response.data.replay ? "This exact feed was already applied." : `${applied} verified offer${applied === 1 ? "" : "s"} applied to the catalog.`);
      await loadHistory();
    } catch (error) {
      setState("error");
      setMessage(error.response?.data?.error || "The feed could not be applied");
    }
  };

  const acceptedRows = useMemo(() => preview?.rows?.filter((row) => row.status === "accepted") || [], [preview]);

  return (
    <div className="offer-admin-page" aria-busy={state === "previewing" || state === "applying"}>
      <header className="offer-admin-hero">
        <div>
          <p className="ui-kicker">Catalog operations</p>
          <h1>Review every price before it becomes live.</h1>
          <p>Upload an authorized retailer or partner feed. ForgeSavant validates the contract, matches curated component models, and changes nothing until you approve the preview.</p>
        </div>
        <div className="offer-admin-rule" aria-label="Import guarantees">
          <FiDatabase aria-hidden="true" />
          <strong>Specifications stay curated</strong>
          <span>Only price, availability, image, source, and observation time can change.</span>
        </div>
      </header>

      <div className="offer-admin-layout">
        <form className="offer-upload-panel" onSubmit={previewFeed}>
          <div className="offer-panel-heading">
            <div><span>01</span><h2>Load a feed</h2></div>
            <a href="/offer-feed-template.csv" download><FiDownload aria-hidden="true" /> Template</a>
          </div>
          <label>
            <span>Source identifier</span>
            <input value={source} onChange={(event) => { setSource(event.target.value.toLowerCase()); setPreview(null); setResolutions({}); setResolutionsDirty(false); }} placeholder="retailer_partner" pattern="[a-z0-9][a-z0-9._-]{1,63}" required />
          </label>
          <label className="offer-dropzone">
            <FiUploadCloud aria-hidden="true" />
            <strong>{fileName || "Choose CSV or JSON"}</strong>
            <span>{offers.length ? `${offers.length} record${offers.length === 1 ? "" : "s"} loaded` : "Maximum 500 records · 1 MB request limit"}</span>
            <input type="file" accept=".csv,.json,text/csv,application/json" onChange={handleFile} required />
          </label>
          <button className="offer-primary" type="submit" disabled={!source || !offers.length || state === "previewing" || state === "applying"}>
            {state === "previewing" ? "Reviewing…" : "Preview matches"}
          </button>
          <div aria-live="polite">{message ? <p className={`offer-message ${state === "error" ? "error" : "success"}`} role={state === "error" ? "alert" : "status"}>{message}</p> : null}</div>
        </form>

        <section className="offer-review-panel" aria-labelledby="offer-review-title">
          <div className="offer-panel-heading">
            <div><span>02</span><h2 id="offer-review-title">Review matches</h2></div>
            {preview ? <code>{preview.checksum.slice(0, 10)}</code> : null}
          </div>
          {!preview ? (
            <div className="offer-empty"><FiAlertTriangle aria-hidden="true" /><strong>No preview yet</strong><span>Upload a feed to see accepted, ambiguous, unmatched, and rejected rows.</span></div>
          ) : (
            <>
              <dl className="offer-counts">
                <div><dt>Ready</dt><dd>{preview.counts.accepted}</dd></div>
                <div><dt>Review</dt><dd>{preview.counts.ambiguous}</dd></div>
                <div><dt>No match</dt><dd>{preview.counts.unmatched}</dd></div>
                <div><dt>Rejected</dt><dd>{preview.counts.rejected}</dd></div>
              </dl>
              <div className="offer-table-wrap">
                <table className="offer-table">
                  <thead><tr><th>Status</th><th>Feed product</th><th>Catalog match</th><th>Price</th></tr></thead>
                  <tbody>{preview.rows.map((row) => (
                    <tr key={`${row.index}-${row.offer.source_item_id}`}>
                      <td><span className={`offer-status ${row.status}`}>{row.status === "accepted" ? <FiCheck aria-hidden="true" /> : null}{statusCopy[row.status]}</span></td>
                      <td><strong>{row.offer.name || "Missing name"}</strong><small>{row.offer.source_item_id || "No source ID"}</small></td>
                      <td>
                        {row.match ? (
                          <><strong>{row.match.name}</strong><small>{row.matchedBy === "saved_mapping" ? "Saved retailer mapping" : row.matchMethod === "manual" ? "Manually resolved" : row.matchedBy === "manufacturer_part_number" ? "Exact manufacturer part number" : `${Math.round((row.score || 0) * 100)}% token match`}</small></>
                        ) : row.candidates?.length ? (
                          <label className="offer-resolution">
                            <span>{row.reason}</span>
                            <select
                              aria-label={`Catalog match for ${row.offer.name}`}
                              value={resolutions[row.index] || ""}
                              onChange={(event) => {
                                setResolutions((current) => ({ ...current, [row.index]: event.target.value }));
                                setResolutionsDirty(true);
                                setState("ready");
                              }}
                            >
                              <option value="">Choose a catalog product</option>
                              {row.candidates.map((candidate) => <option key={candidate.id} value={candidate.id}>{candidate.name} ({Math.round(candidate.score * 100)}%)</option>)}
                            </select>
                          </label>
                        ) : <strong>{row.reason || row.errors?.join(" · ")}</strong>}
                      </td>
                      <td>₹{Number(row.offer.price || 0).toLocaleString("en-IN")}</td>
                    </tr>
                  ))}</tbody>
                </table>
              </div>
              <div className="offer-apply-bar">
                <span>{acceptedRows.length} verified row{acceptedRows.length === 1 ? "" : "s"} will be applied. Other rows remain unchanged.</span>
                <div className="offer-apply-actions">
                  {resolutionsDirty ? <button className="offer-secondary" type="button" onClick={previewFeed} disabled={state === "previewing" || state === "applying"}>Review selected mappings</button> : null}
                  <button className="offer-primary" type="button" onClick={applyFeed} disabled={!acceptedRows.length || resolutionsDirty || state === "applying" || state === "applied"}>
                    {state === "applying" ? "Applying…" : state === "applied" ? "Applied" : "Apply verified offers"}
                  </button>
                </div>
              </div>
            </>
          )}
        </section>
      </div>

      <section className="offer-history">
        <div className="offer-panel-heading"><div><span>03</span><h2>Recent imports</h2></div></div>
        {history.length ? <div className="offer-history-list">{history.map((batch) => (
          <article key={batch._id}><strong>{batch.source}</strong><span>{new Date(batch.createdAt).toLocaleString()}</span><span>{batch.counts.applied} applied</span><code>{batch.checksum.slice(0, 10)}</code></article>
        ))}</div> : <p>No partner feeds have been applied yet.</p>}
      </section>
    </div>
  );
};

export default AdminOffers;
