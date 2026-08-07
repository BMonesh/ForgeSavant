import { FiArrowRight, FiBarChart2, FiCheckCircle, FiDatabase, FiShield } from "react-icons/fi";
import { Link } from "react-router-dom";
import "../Styles/About.css";

const evidenceLanes = [
  { index: "01", title: "Compatibility catalog", source: "Verified identity + curated specifications", use: "Socket, memory, power and enclosure decisions", state: "Operational" },
  { index: "02", title: "Product content", source: "Open Icecat + reviewed manufacturer pages", use: "Identity, inspection and source context", state: "Coverage-limited" },
  { index: "03", title: "Performance evidence", source: "Licensed or open benchmark observations", use: "Same-workload comparison, never universal ranking", state: "Workload-limited" },
  { index: "04", title: "Retail offers", source: "Authorized retailer or partner feeds", use: "Current INR price and availability", state: "Source-required" },
  { index: "05", title: "Build outcomes", source: "Explicitly consented, pseudonymous events", use: "Future recommendation evaluation", state: "Opt-in only" },
];

const About = () => (
  <article className="about-page">
    <header className="about-hero">
      <div>
        <p className="ui-kicker">About / evidence-backed planning</p>
        <h1>A PC plan should be inspectable.</h1>
        <p className="about-lede">ForgeSavant is a guided custom-PC planning application and an auditable data platform. It helps people choose compatible parts while preserving the boundary between verified facts, planning estimates, public benchmarks, retailer evidence, and commercial links.</p>
        <div className="about-actions">
          <Link to="/build">Open the builder <FiArrowRight aria-hidden="true" /></Link>
          <Link to="/benchmarks">Inspect benchmarks</Link>
        </div>
      </div>
      <aside className="about-principle" aria-label="Core product principle">
        <FiShield aria-hidden="true" />
        <span>Core principle</span>
        <strong>No source gets to prove more than it actually contains.</strong>
      </aside>
    </header>

    <section className="about-method" aria-labelledby="about-method-title">
      <div className="about-section-intro">
        <p className="ui-kicker">Method / 01</p>
        <h2 id="about-method-title">Five evidence lanes. Separate responsibilities.</h2>
        <p>A product-content record cannot become a retailer price. An affiliate destination cannot influence compatibility. A benchmark cannot become a personalized recommendation without outcome evidence.</p>
      </div>
      <div className="about-evidence-table" role="list">
        {evidenceLanes.map((lane) => (
          <article key={lane.index} role="listitem">
            <span>{lane.index}</span>
            <div><h3>{lane.title}</h3><p>{lane.source}</p></div>
            <p>{lane.use}</p>
            <strong>{lane.state}</strong>
          </article>
        ))}
      </div>
    </section>

    <section className="about-pillars" aria-label="ForgeSavant commitments">
      <article><FiCheckCircle aria-hidden="true" /><span>Compatibility</span><h2>Rules before recommendations.</h2><p>The builder evaluates explicit catalog attributes and shows the rule behind each allowed or blocked choice.</p></article>
      <article><FiDatabase aria-hidden="true" /><span>Data platform</span><h2>Immutable observations.</h2><p>Source evidence lands as validated observations with timestamps, identifiers, checksums, quality metrics and a review trail.</p></article>
      <article><FiBarChart2 aria-hidden="true" /><span>Data science</span><h2>Readiness before modeling.</h2><p>Predictive work stays blocked until coverage, history, labels and leakage-safe evaluation are sufficient for the claimed use.</p></article>
    </section>

    <section className="about-boundary" aria-labelledby="commercial-boundary-title">
      <div><p className="ui-kicker">Commercial boundary / 02</p><h2 id="commercial-boundary-title">Paid links remain destinations—not evidence.</h2></div>
      <div><p>Amazon Associate and future partner links are stored separately from compatibility, benchmark ranking, catalog specifications, and retailer-price observations. Commission cannot make a component appear more compatible or better performing.</p><Link to="/affiliate-disclosure">Read the affiliate disclosure <FiArrowRight aria-hidden="true" /></Link></div>
    </section>
  </article>
);

export default About;
