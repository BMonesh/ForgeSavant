import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { FiArrowRight, FiCpu, FiEdit3, FiPlus, FiTrash2 } from "react-icons/fi";
import "../Styles/Profile.css";
import api from "../services/api";
import ConfirmDialog from "./ui/ConfirmDialog";
import { useSession } from "../auth/SessionContext";
import fallbackCase from "../assets/custom-gaming-pc.png";

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

      {error && status !== "error" ? <p className="profile-inline-error" role="alert">{error}</p> : null}

      {status === "loading" ? (
        <section className="profile-state" aria-live="polite">
          <span className="profile-loader" aria-hidden="true" />
          <strong>Loading saved builds</strong>
          <p>Retrieving your private build records.</p>
        </section>
      ) : status === "error" ? (
        <section className="profile-state" role="alert">
          <strong>Saved builds unavailable</strong>
          <p>{error}</p>
          <button type="button" className="profile-action" onClick={loadBuilds}>Retry</button>
        </section>
      ) : builds.length > 0 ? (
        <section className="saved-build-list" aria-label="Saved builds">
          {builds.map((item, index) => (
            <article key={item._id} className="saved-build">
              <div className="saved-build-visual">
                <span>Build {String(index + 1).padStart(2, "0")}</span>
                <img src={item.image || fallbackCase} onError={(event) => { event.currentTarget.onerror = null; event.currentTarget.src = fallbackCase; }} alt={item.cabinet || "Saved PC build"} />
              </div>
              <div className="saved-build-content">
                <div className="saved-build-title">
                  <div><span>Compatible configuration</span><h2>{item.cpu || "Saved build"}</h2></div>
                  <FiCpu aria-hidden="true" />
                </div>
                <dl className="saved-parts">
                  {buildRows.map(([label, key]) => <div key={key}><dt>{label}</dt><dd>{item[key] || "Not recorded"}</dd></div>)}
                </dl>
                <div className="saved-build-footer">
                  <div className="saved-metrics">
                    {item.analytics?.performance ? (
                      <>
                        <span>CPU planning index <strong>{item.analytics.performance.cpuParallelismIndex}</strong></span>
                        <span>GPU memory <strong>{item.analytics.performance.gpuMemoryGB} GB</strong></span>
                      </>
                    ) : (
                      <span>Legacy estimate <strong>Reopen to recalculate</strong></span>
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
