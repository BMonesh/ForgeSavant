import { FiArrowRight, FiCompass } from "react-icons/fi";
import { Link } from "react-router-dom";
import "../Styles/NotFound.css";

const NotFound = () => (
  <section className="not-found-page" aria-labelledby="not-found-title">
    <div className="not-found-grid" aria-hidden="true" />
    <div className="not-found-copy">
      <p className="ui-kicker">Route check / 404</p>
      <span className="not-found-code">404</span>
      <h1 id="not-found-title">This path is not in the build plan.</h1>
      <p>The address may be outdated or incomplete. Your saved configurations and current builder draft have not been changed.</p>
      <div className="not-found-actions">
        <Link className="not-found-primary" to="/">Return home <FiArrowRight aria-hidden="true" /></Link>
        <Link to="/build">Open builder</Link>
        <Link to="/benchmarks">View benchmarks</Link>
      </div>
    </div>
    <aside className="not-found-note" aria-label="Recovery options">
      <FiCompass aria-hidden="true" />
      <span>Recovery paths</span>
      <strong>Choose a verified destination.</strong>
      <p>ForgeSavant does not guess which page you intended to open.</p>
    </aside>
  </section>
);

export default NotFound;
