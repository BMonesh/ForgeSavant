import { Link } from "react-router-dom";
import "../Styles/Legal.css";

const AffiliateDisclosure = () => (
  <article className="legal-page">
    <header>
      <p className="ui-kicker">Transparency / retailer destinations</p>
      <h1>Affiliate disclosure</h1>
      <p>ForgeSavant separates product evidence, planning estimates, and paid retailer destinations so you can see what each source contributes.</p>
    </header>
    <section className="legal-notice" aria-labelledby="amazon-associates-title">
      <span className="legal-evidence">Paid destination</span>
      <h2 id="amazon-associates-title">Amazon Associates</h2>
      <p><strong>As an Amazon Associate I earn from qualifying purchases.</strong></p>
      <p>Some links to Amazon.in are affiliate links. If you follow one and make a qualifying purchase, ForgeSavant may receive a commission without increasing the price you pay.</p>
    </section>
    <section aria-labelledby="recommendation-independence-title">
      <h2 id="recommendation-independence-title">How retailer links affect recommendations</h2>
      <p>Affiliate availability does not establish compatibility, benchmark performance, ranking, or a live market price. Compatibility is evaluated from the catalog’s structured component evidence. Amazon price and availability must be confirmed on Amazon.in.</p>
    </section>
    <Link className="legal-back" to="/">Return home</Link>
  </article>
);

export default AffiliateDisclosure;
