import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { FiArrowRight, FiEdit3, FiPlus, FiTrash2 } from "react-icons/fi";
import "../Styles/Profile.css";
import api from "../services/api";
import ConfirmDialog from "./ui/ConfirmDialog";
import { useSession } from "../auth/SessionContext";

const buildRows = [
  ["CPU", "cpu"],
  ["Motherboard", "motherboard"],
  ["GPU", "gpu"],
  ["Primary storage", "primaryStorage"],
  ["Secondary storage", "secondaryStorage"],
  ["RAM", "ram"],
  ["PSU", "powerSupply"],
  ["Cabinet", "cabinet"],
];

const Profile = () => {
  const [builds, setBuilds] = useState([]);
  const [status, setStatus] = useState("loading");
  const [error, setError] = useState("");
  const [pendingDelete, setPendingDelete] = useState(null);
  const [deleting, setDeleting] = useState(false);
  const [analyticsConsent, setAnalyticsConsent] = useState(null);
  const [consentSaving, setConsentSaving] = useState(false);
  const [consentMessage, setConsentMessage] = useState("");
  const { user } = useSession();

  const loadBuilds = useCallback(async () => {
    setStatus("loading");
    setError("");
    try {
      const response = await api.get("/saves2");
      setBuilds(response.data);
      setStatus("ready");
    } catch (requestError) {
      console.error("Error fetching saved builds:", requestError);
      setError(requestError.response?.data?.error || "Unable to load saved builds.");
      setStatus("error");
    }
  }, []);

  useEffect(() => {
    loadBuilds();
  }, [loadBuilds]);

  useEffect(() => {
    api.get("/api/v1/privacy/analytics")
      .then((response) => setAnalyticsConsent(Boolean(response.data.data.enabled)))
      .catch(() => setConsentMessage("Privacy preference is temporarily unavailable."));
  }, []);

  const handleConsentChange = async (event) => {
    const enabled = event.target.checked;
    setConsentSaving(true);
    setConsentMessage("");
    try {
      const response = await api.patch("/api/v1/privacy/analytics", { enabled });
      setAnalyticsConsent(Boolean(response.data.data.enabled));
      setConsentMessage(enabled
        ? "Anonymous build-outcome learning is enabled."
        : "Learning is disabled and existing anonymous outcome events were removed.");
    } catch (requestError) {
      setConsentMessage(requestError.response?.data?.error || "Unable to update the privacy preference.");
    } finally {
      setConsentSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!pendingDelete) return;
    setDeleting(true);
    setError("");

    try {
      await api.delete(`/delsaves/${pendingDelete._id}`);
      setBuilds((current) => current.filter((item) => item._id !== pendingDelete._id));
      setPendingDelete(null);
    } catch (requestError) {
      console.error("Error deleting saved build:", requestError);
      setError(requestError.response?.data?.error || "Unable to remove this build.");
      setPendingDelete(null);
    } finally {
      setDeleting(false);
    }
  };

  return (
    <div className="profile-page">
      <header className="profile-header">
        <div>
          <p className="ui-kicker">{user?.fullname || "Builder"} / saved configurations</p>
          <h1>Build history</h1>
          <p>Review compatible configurations stored against your account.</p>
        </div>
        <Link to="/build" state={{ newBuild: true }} className="profile-action"><FiPlus aria-hidden="true" /> New build</Link>
      </header>

      <section className="profile-privacy" aria-labelledby="analytics-consent-title">
        <div>
          <p className="ui-kicker">Privacy / optional research</p>
          <h2 id="analytics-consent-title">Help improve build recommendations</h2>
          <p>If enabled, saving or updating a build records its component IDs, total, compatibility engine version, and planning-model version under a one-way pseudonym. ForgeSavant does not include your name, email, searches, or page views.</p>
        </div>
        <label className="privacy-switch">
          <input
            type="checkbox"
            checked={Boolean(analyticsConsent)}
            onChange={handleConsentChange}
            disabled={analyticsConsent == null || consentSaving}
          />
          <span aria-hidden="true" />
          <strong>{analyticsConsent ? "Enabled" : analyticsConsent == null ? "Loading" : "Disabled"}</strong>
        </label>
        {consentMessage ? <p className="privacy-message" role="status">{consentMessage}</p> : null}
      </section>

      {error && status !== "error" ? <p className="profile-inline-error" role="alert">{error}</p> : null}

      {status === "loading" ? (
        <section className="profile-state profile-loading" aria-live="polite" aria-busy="true">
          <span className="sr-only">Loading saved builds</span>
          <div className="profile-skeleton-row" aria-hidden="true" />
          <div className="profile-skeleton-row" aria-hidden="true" />
          <div className="profile-skeleton-row short" aria-hidden="true" />
        </section>
      ) : status === "error" ? (
        <section className="profile-state" role="alert">
          <strong>Saved builds unavailable</strong>
          <p>{error}</p>
          <button type="button" className="profile-action" onClick={loadBuilds}>Retry</button>
        </section>
      ) : builds.length > 0 ? (
        <section className="saved-build-list" aria-label="Saved builds">
          <div className="saved-build-list-head" aria-hidden="true">
            <span>Record</span><span>Configuration evidence</span><span>Actions</span>
          </div>
          {builds.map((item, index) => (
            <article key={item._id} className="saved-build">
              <div className="saved-build-index" aria-label={`Build ${index + 1}`}>
                <span>{String(index + 1).padStart(2, "0")}</span>
                <small>Saved</small>
              </div>
              <div className="saved-build-content">
                <div className="saved-build-title">
                  <div><span>Saved configuration · Review on reopen</span><h2>{item.cpu || "Unnamed saved build"}</h2></div>
                </div>
                <dl className="saved-parts">
                  {buildRows.map(([label, key]) => <div key={key}><dt>{label}</dt><dd>{item[key] || "Not recorded"}</dd></div>)}
                </dl>
                <div className="saved-build-footer">
                  <div className="saved-metrics">
                    {item.analytics?.performance ? (
                      <>
                        <span><em>Planning</em> CPU index <strong>{item.analytics.performance.cpuParallelismIndex ?? "Unavailable"}</strong></span>
                        <span><em>Recorded</em> GPU memory <strong>{item.analytics.performance.gpuMemoryGB != null ? `${item.analytics.performance.gpuMemoryGB} GB` : "Unavailable"}</strong></span>
                      </>
                    ) : (
                      <span><em>Stale</em> Legacy estimate <strong>Reopen to recalculate</strong></span>
                    )}
                  </div>
                  <div className="saved-actions">
                    <Link to="/build" state={{ savedBuild: item }} className="open-build"><FiEdit3 aria-hidden="true" /> Open build</Link>
                    <button type="button" className="remove-build" onClick={() => setPendingDelete(item)}><FiTrash2 aria-hidden="true" /> Remove</button>
                  </div>
                </div>
              </div>
            </article>
          ))}
        </section>
      ) : (
        <section className="profile-state">
          <span className="empty-index">00</span>
          <strong>No saved builds</strong>
          <p>Complete the compatibility sequence and save the review when it is ready.</p>
          <Link to="/build" state={{ newBuild: true }} className="profile-action">Open builder <FiArrowRight aria-hidden="true" /></Link>
        </section>
      )}

      <ConfirmDialog open={Boolean(pendingDelete)} title="Remove saved build?" description={`This permanently removes ${pendingDelete?.cpu || "this configuration"} from your history.`} busy={deleting} onCancel={() => setPendingDelete(null)} onConfirm={handleDelete} />
    </div>
  );
};

export default Profile;
