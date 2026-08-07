import { FiArrowDown, FiArrowRight, FiCheckCircle, FiDatabase, FiLock, FiRefreshCw } from "react-icons/fi";
import { Link } from "react-router-dom";
import "../Styles/Partners.css";

const requiredFields = [
  ["Product identity", "Manufacturer, exact model and manufacturer part number"],
  ["Offer", "INR price, availability and product URL"],
  ["Retailer identity", "Retailer name and stable retailer product ID"],
  ["Observation", "Collection timestamp and source/feed identifier"],
];

const Partners = () => (
  <div className="partners-page">
    <header className="partners-hero">
      <div>
        <p className="ui-kicker">Retail data partnership / India</p>
        <h1>Help builders compare current PC-part offers with evidence.</h1>
        <p>ForgeSavant is seeking authorized retailer feeds for PC components sold in India. Every offer is validated, matched to an exact catalog identity, timestamped, and kept separate from compatibility and benchmark evidence.</p>
        <div className="partners-actions">
          <a className="partners-primary" href="mailto:2005.monesh@gmail.com?subject=ForgeSavant%20retail%20data%20partnership">Discuss a data feed <FiArrowRight aria-hidden="true" /></a>
          <a href="/offer-feed-template.csv" download>Download CSV template <FiArrowDown aria-hidden="true" /></a>
        </div>
      </div>
      <aside aria-label="Partnership status">
        <FiDatabase aria-hidden="true" />
        <span>Current request</span>
        <strong>Authorized price and availability observations</strong>
        <p>CSV, JSON or documented API. A sample feed is sufficient for technical validation.</p>
      </aside>
    </header>

    <main className="partners-body">
      <section className="partners-fields" aria-labelledby="partner-fields-title">
        <div className="partners-section-heading"><span>01</span><div><h2 id="partner-fields-title">Minimum useful fields</h2><p>No customer or order data is requested.</p></div></div>
        <div className="partners-field-grid">
          {requiredFields.map(([title, detail], index) => <article key={title}><span>{String(index + 1).padStart(2, "0")}</span><h3>{title}</h3><p>{detail}</p></article>)}
        </div>
      </section>

      <section className="partners-process" aria-labelledby="partner-process-title">
        <div className="partners-section-heading"><span>02</span><div><h2 id="partner-process-title">How evidence enters ForgeSavant</h2><p>Import is review-first and reversible at the source boundary.</p></div></div>
        <ol>
          <li><FiDatabase aria-hidden="true" /><div><strong>Receive</strong><p>The supplied file or API response is stored as a source-specific observation.</p></div></li>
          <li><FiCheckCircle aria-hidden="true" /><div><strong>Validate</strong><p>URLs, currency, timestamps, identifiers and values must pass the feed contract.</p></div></li>
          <li><FiRefreshCw aria-hidden="true" /><div><strong>Match and review</strong><p>Exact manufacturer part numbers are preferred; ambiguous products require operator review.</p></div></li>
          <li><FiLock aria-hidden="true" /><div><strong>Publish with provenance</strong><p>Accepted offers retain retailer, source URL, collection time and freshness status.</p></div></li>
        </ol>
      </section>

      <section className="partners-boundaries" aria-labelledby="partner-boundaries-title">
        <div><p className="ui-kicker">Evidence boundaries / 03</p><h2 id="partner-boundaries-title">Commercial relationships do not control technical results.</h2></div>
        <ul>
          <li>Retail price data cannot alter compatibility rules.</li>
          <li>Affiliate links cannot become price observations.</li>
          <li>Every live offer expires when its freshness window passes.</li>
          <li>Source attribution remains visible on component detail pages.</li>
        </ul>
      </section>

      <section className="partners-contact" aria-labelledby="partner-contact-title">
        <span>04</span>
        <div><h2 id="partner-contact-title">Start with a sample.</h2><p>Send ten to fifty component offers or API documentation. ForgeSavant will return a validation and identity-match report before any data is published.</p></div>
        <a href="mailto:2005.monesh@gmail.com?subject=ForgeSavant%20retail%20data%20partnership">2005.monesh@gmail.com <FiArrowRight aria-hidden="true" /></a>
      </section>

      <nav className="partners-related" aria-label="Related methodology">
        <Link to="/about">Read the data methodology</Link>
        <Link to="/benchmarks">Inspect benchmark evidence</Link>
        <Link to="/affiliate-disclosure">Review affiliate separation</Link>
      </nav>
    </main>
  </div>
);

export default Partners;
