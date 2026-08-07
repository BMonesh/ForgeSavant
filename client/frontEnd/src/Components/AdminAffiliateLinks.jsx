import { useEffect, useMemo, useState } from "react";
import { FiAlertTriangle, FiCheck, FiDownload, FiExternalLink, FiLink, FiUploadCloud } from "react-icons/fi";
import api from "../services/api";
import { parseAffiliateLinkFile } from "./admin/affiliateLinkFile";
import "../Styles/AdminOffers.css";

const statusCopy = {
  accepted: "Exact match",
  ambiguous: "Ambiguous",
  unmatched: "No match",
  rejected: "Rejected",
};

const AdminAffiliateLinks = () => {
  const [links, setLinks] = useState([]);
  const [fileName, setFileName] = useState("");
  const [preview, setPreview] = useState(null);
  const [history, setHistory] = useState([]);
  const [state, setState] = useState("idle");
  const [message, setMessage] = useState("");

  const accepted = useMemo(
    () => preview?.rows?.filter((row) => row.status === "accepted") || [],
    [preview]
  );

  const loadHistory = () => api.get("/api/v1/admin/affiliate-links/history")
    .then((response) => setHistory(response.data.data || []))
    .catch(() => {});

  useEffect(() => { loadHistory(); }, []);

  const handleFile = async (event) => {
    setPreview(null);
    setMessage("");
    try {
      const file = event.target.files?.[0];
      if (!file) return;
      if (file.size > 1024 * 1024) throw new Error("The link feed must be 1 MB or smaller");
      const parsed = parseAffiliateLinkFile(await file.text());
      if (parsed.length > 500) throw new Error("A link feed can contain at most 500 records");
      setLinks(parsed);
      setFileName(file.name);
      setState("ready");
    } catch (error) {
      setLinks([]);
      setFileName("");
      setState("error");
      setMessage(error.message);
    }
  };

  const previewFeed = async (event) => {
    event.preventDefault();
    setState("previewing");
    setMessage("");
    try {
      const response = await api.post("/api/v1/admin/affiliate-links/preview", { links });
      setPreview(response.data.data);
      setState("reviewed");
    } catch (error) {
      setState("error");
      setMessage(error.response?.data?.error || "Unable to review the Amazon link feed.");
    }
  };

  const applyFeed = async () => {
    setState("applying");
    setMessage("");
    try {
      const response = await api.post("/api/v1/admin/affiliate-links/apply", {
        links,
        previewToken: preview.previewToken,
      });
      setState("applied");
      setMessage(response.data.replay
        ? "This exact link feed was already applied."
        : `${response.data.data.counts.applied} reviewed Amazon destination link${response.data.data.counts.applied === 1 ? "" : "s"} applied.`);
      loadHistory();
    } catch (error) {
      setState("error");
      setMessage(error.response?.data?.error || "Unable to apply the reviewed links.");
    }
  };

  return (
    <div className="offer-admin-page" aria-busy={state === "previewing" || state === "applying"}>
      <header className="offer-admin-hero">
        <div>
          <p className="ui-kicker">Affiliate destination operations</p>
          <h1>Link products without inventing prices.</h1>
          <p>Map reviewed Amazon.in ASINs to exact catalog identities. These records create paid destinations only—never product specifications, availability, or retail-price observations.</p>
        </div>
        <div className="offer-admin-rule" aria-label="Amazon link guarantees">
          <FiLink aria-hidden="true" />
          <strong>Exact identity matches only</strong>
          <span>Fixed Amazon.in host, configured Associate tag, and no storefront scraping.</span>
        </div>
      </header>

      <div className="offer-admin-layout">
        <form className="offer-upload-panel" onSubmit={previewFeed}>
          <div className="offer-panel-heading">
            <div><span>01</span><h2>Load ASIN mappings</h2></div>
            <a href="/amazon-link-feed-template.json" download><FiDownload aria-hidden="true" /> Template</a>
          </div>
          <label className="offer-dropzone">
            <FiUploadCloud aria-hidden="true" />
            <strong>{fileName || "Choose Amazon link JSON"}</strong>
            <span>{links.length ? `${links.length} mapping${links.length === 1 ? "" : "s"} loaded` : "Maximum 500 records · no prices or images"}</span>
            <input type="file" accept=".json,application/json" onChange={handleFile} required />
          </label>
          <button className="offer-primary" type="submit" disabled={!links.length || state === "previewing" || state === "applying"}>
            {state === "previewing" ? "Reviewing…" : "Preview exact matches"}
          </button>
          <div aria-live="polite">{message ? <p className={`offer-message ${state === "error" ? "error" : "success"}`} role={state === "error" ? "alert" : "status"}>{message}</p> : null}</div>
        </form>

        <section className="offer-review-panel" aria-labelledby="affiliate-review-title">
          <div className="offer-panel-heading">
            <div><span>02</span><h2 id="affiliate-review-title">Review destinations</h2></div>
            {preview ? <code>{preview.checksum.slice(0, 10)}</code> : null}
          </div>
          {!preview ? (
            <div className="offer-empty">
              <FiAlertTriangle aria-hidden="true" />
              <strong>No preview yet</strong>
              <span>Every ASIN must match an exact component ID or verified manufacturer part number.</span>
            </div>
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
                  <thead><tr><th>Status</th><th>ASIN</th><th>Catalog match</th><th>Destination</th></tr></thead>
                  <tbody>{preview.rows.map((row) => (
                    <tr key={`${row.index}-${row.link.asin}`}>
                      <td><span className={`offer-status ${row.status}`}>{row.status === "accepted" ? <FiCheck aria-hidden="true" /> : null}{statusCopy[row.status]}</span></td>
                      <td><strong>{row.link.asin || "Invalid ASIN"}</strong><small>{row.link.manufacturer_part_number || row.link.component_id}</small></td>
                      <td>{row.match ? <><strong>{row.match.name}</strong><small>{row.matchedBy.replace(/_/g, " ")}</small></> : <strong>{row.reason || row.errors?.join(" · ")}</strong>}</td>
                      <td>{row.link.source_url ? <a href={row.link.source_url} target="_blank" rel="noreferrer sponsored nofollow">Amazon.in <FiExternalLink aria-hidden="true" /></a> : "Not generated"}</td>
                    </tr>
                  ))}</tbody>
                </table>
              </div>
              <div className="offer-apply-bar">
                <span>{accepted.length} exact destination{accepted.length === 1 ? "" : "s"} can be published. No price records will be written.</span>
                <button className="offer-primary" type="button" onClick={applyFeed} disabled={!accepted.length || state === "applying" || state === "applied"}>
                  {state === "applying" ? "Applying…" : state === "applied" ? "Applied" : "Publish reviewed links"}
                </button>
              </div>
            </>
          )}
        </section>
      </div>

      <section className="offer-history">
        <div className="offer-panel-heading"><div><span>03</span><h2>Recent link imports</h2></div></div>
        {history.length ? <div className="offer-history-list">{history.map((batch) => (
          <article key={batch._id}><strong>{batch.source}</strong><span>{new Date(batch.createdAt).toLocaleString()}</span><span>{batch.counts.applied} applied</span><code>{batch.checksum.slice(0, 10)}</code></article>
        ))}</div> : <p>No Amazon destination mappings have been published yet.</p>}
      </section>
    </div>
  );
};

export default AdminAffiliateLinks;
