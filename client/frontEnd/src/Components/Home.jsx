import { useEffect, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import {
  FiArrowRight,
  FiCheck,
  FiChevronDown,
  FiCpu,
  FiDatabase,
  FiHardDrive,
  FiServer,
  FiShield,
  FiTool,
  FiZap,
} from "react-icons/fi";
import "../Styles/home.css";
import heroCase from "../assets/forgesavant-exploded-case-transparent.webp";
import featuredBuild from "../assets/forgesavant-featured-build-transparent.webp";
import processorImg from "../assets/Processor-Background-PNG-Image.webp";
import motherboardImg from "../assets/Motherboard-PNG.webp";
import gpuImg from "../assets/graphics-card-image.webp";
import ramImg from "../assets/RAM-Memory-Transparent.webp";
import storageImg from "../assets/pngwing.com.webp";
import psuImg from "../assets/SMPS-image.webp";
import api from "../services/api";
import BrandLogo from "./ui/BrandLogo";

const categories = [
  { key: "processors", name: "Processors", noun: "in catalog", image: processorImg, icon: FiCpu },
  { key: "gpus", name: "Graphics cards", noun: "in catalog", image: gpuImg, icon: FiZap },
  { key: "motherboards", name: "Motherboards", noun: "in catalog", image: motherboardImg, icon: FiServer },
  { key: "ram", name: "Memory", noun: "kits", image: ramImg, icon: FiDatabase },
  { key: "storage", name: "Storage", noun: "drives", image: storageImg, icon: FiHardDrive },
  { key: "powerSupplies", name: "Power supplies", noun: "verified", image: psuImg, icon: FiShield },
];

const faqs = [
  {
    question: "How does ForgeSavant decide whether parts are compatible?",
    answer: "The builder checks explicit component attributes such as CPU socket, memory generation, motherboard form factor and estimated power draw. Every decision shows the rule that allowed or blocked it.",
  },
  {
    question: "Are the prices live retailer prices?",
    answer: "Most catalog prices are still clearly labeled planning data. Authorized partner-feed observations are labeled live or stale with their retailer, timestamp, availability and historical record.",
  },
  {
    question: "Can I save and revise a configuration?",
    answer: "Yes. Sign in, complete the compatibility sequence and save the build. Saved configurations can be reopened, revised and removed from your private build history.",
  },
  {
    question: "Are performance numbers guaranteed benchmarks?",
    answer: "No. Current numbers are planning estimates, not measured guarantees. ForgeSavant labels estimates clearly while the benchmark dataset is expanded.",
  },
];

const Home = () => {
  const location = useLocation();
  const [openFaq, setOpenFaq] = useState(0);
  const [catalogMeta, setCatalogMeta] = useState({ total: null, counts: {} });

  useEffect(() => {
    let active = true;
    api.get("/api/v1/catalog")
      .then((response) => { if (active) setCatalogMeta(response.data.meta); })
      .catch(() => {});
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (!location.hash) return;
    const target = document.querySelector(location.hash);
    const reduceMotion = window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
    requestAnimationFrame(() => target?.scrollIntoView({ behavior: reduceMotion ? "auto" : "smooth" }));
  }, [location.hash]);

  return (
    <div className="home-page">
      <section className="home-hero" aria-labelledby="home-title">
        <div className="home-hero-copy">
          <p className="home-eyebrow">PC planning, without the guesswork</p>
          <h1 id="home-title">Build the right PC.<br />Know why it works.</h1>
          <p className="home-lede">
            Explore a structured component catalog, see compatibility evidence,
            and assemble a configuration around your workload and budget.
          </p>
          <div className="home-actions">
            <Link to="/build" state={{ newBuild: true }} className="home-button home-button-dark">
              Start a build <FiArrowRight aria-hidden="true" />
            </Link>
            <a href="#how-it-works" className="home-button home-button-light">See how it works</a>
          </div>
          <dl className="home-proof">
            <div><dt>{catalogMeta.total ?? "—"}</dt><dd>catalog parts</dd></div>
            <div><dt>4</dt><dd>core rule families</dd></div>
            <div><dt>₹</dt><dd>price provenance</dd></div>
          </dl>
        </div>

        <div className="home-hero-art">
          <img src={heroCase} alt="Exploded view of a modular desktop PC case" />
          <div className="hero-note hero-note-top"><span>01</span> Start from workload</div>
          <div className="hero-note hero-note-bottom"><span>02</span> Verify every fit</div>
        </div>

        <aside className="home-hero-aside" aria-label="ForgeSavant modes">
          <div>
            <span>Guided builder</span>
            <h2>Choose with constraints</h2>
            <p>Each choice narrows the next list using visible compatibility rules.</p>
          </div>
          <div>
            <span>Build library</span>
            <h2>Return to decisions</h2>
            <p>Save a configuration, reopen it later and replace parts without starting over.</p>
          </div>
        </aside>
      </section>

      <section className="featured-build" id="recommended" aria-labelledby="featured-title">
        <div className="featured-copy">
          <p className="home-eyebrow">Compatibility evidence / 01</p>
          <h2 id="featured-title">Every choice leaves evidence.</h2>
          <ul>
            <li><FiCheck aria-hidden="true" /> AM5 upgrade path</li>
            <li><FiCheck aria-hidden="true" /> 32 GB DDR5 memory</li>
            <li><FiCheck aria-hidden="true" /> 650 W power target</li>
            <li><FiCheck aria-hidden="true" /> Airflow-first ATX case</li>
          </ul>
          <div className="featured-price">
            <span>Current catalog scope</span>
            <strong>{catalogMeta.total ?? "—"} parts</strong>
            <small>Availability and price provenance are shown per item.</small>
          </div>
          <Link to="/build" state={{ newBuild: true }} className="featured-link">
            Review the builder <FiArrowRight aria-hidden="true" />
          </Link>
        </div>

        <div className="featured-art">
          <img src={featuredBuild} alt="Black performance desktop PC with visible internal components" />
        </div>

        <dl className="featured-specs">
          <div><dt>Source</dt><dd>Catalog API<br />current application data</dd></div>
          <div><dt>Compatibility</dt><dd>Explicit rules<br />visible before advancing</dd></div>
          <div><dt>Pricing</dt><dd>Live, stale, estimated<br />or unavailable</dd></div>
          <div><dt>Saving</dt><dd>Private builds<br />account required</dd></div>
        </dl>
      </section>

      <section className="component-section" id="components" aria-labelledby="component-title">
        <div className="section-heading">
          <div>
            <p className="home-eyebrow">Current catalog</p>
            <h2 id="component-title">Start with the part you understand.</h2>
          </div>
          <p>ForgeSavant turns specifications into a guided sequence, so you do not need to memorize every socket, interface or power rule.</p>
        </div>
        <div className="component-grid">
          {categories.map(({ key, name, noun, image, icon: Icon }) => (
            <Link key={name} to="/build" className="component-card">
              <span className="component-card-icon"><Icon aria-hidden="true" /></span>
              <img src={image} alt="" />
              <span><strong>{name}</strong><small>{catalogMeta.counts?.[key] ?? "—"} {noun}</small></span>
              <FiArrowRight aria-hidden="true" />
            </Link>
          ))}
        </div>
      </section>

      <section className="method-section" id="how-it-works" aria-labelledby="method-title">
        <div className="method-intro">
          <p className="home-eyebrow">How it works</p>
          <h2 id="method-title">A build process you can audit.</h2>
          <p>Recommendations are useful only when you can inspect the assumptions behind them.</p>
          <Link to="/build" className="method-link">Open the workbench <FiArrowRight aria-hidden="true" /></Link>
        </div>
        <ol className="method-steps">
          <li><span>01</span><div><h3>Set a platform</h3><p>Choose AMD or Intel to establish the processor family and socket path.</p></div></li>
          <li><span>02</span><div><h3>Resolve constraints</h3><p>Motherboard, memory, storage, power and enclosure choices are filtered in sequence.</p></div></li>
          <li><span>03</span><div><h3>Review evidence</h3><p>Inspect fit rules, estimated draw, price and any assumptions before saving.</p></div></li>
        </ol>
      </section>

      <section className="faq-section" aria-labelledby="faq-title">
        <div className="faq-heading">
          <p className="home-eyebrow">Useful answers</p>
          <h2 id="faq-title">Before you build.</h2>
        </div>
        <div className="faq-list">
          {faqs.map((faq, index) => {
            const isOpen = openFaq === index;
            return (
              <div className={`faq-item ${isOpen ? "open" : ""}`} key={faq.question}>
                <button
                  type="button"
                  onClick={() => setOpenFaq(isOpen ? -1 : index)}
                  aria-expanded={isOpen}
                  aria-controls={`faq-answer-${index}`}
                >
                  <span>{faq.question}</span><FiChevronDown aria-hidden="true" />
                </button>
                {isOpen ? <p id={`faq-answer-${index}`}>{faq.answer}</p> : null}
              </div>
            );
          })}
        </div>
      </section>

      <section className="home-cta">
        <FiTool aria-hidden="true" />
        <div><p className="home-eyebrow home-eyebrow-dark">Ready when you are</p><h2>Turn a parts list into a defensible build.</h2></div>
        <Link to="/build" state={{ newBuild: true }} className="home-button home-button-light">Launch builder <FiArrowRight aria-hidden="true" /></Link>
      </section>

      <footer className="home-footer">
        <div className="home-footer-brand">
          <BrandLogo title="ForgeSavant" />
          <p>Compatibility-led PC planning.</p>
        </div>
        <div><span>Product</span><Link to="/build">Builder</Link><a href="#recommended">Reference build</a><a href="#components">Catalog</a></div>
        <div><span>Method</span><a href="#how-it-works">How it works</a><a href="#faq-title">FAQ</a><Link to="/about">About</Link><Link to="/affiliate-disclosure">Affiliate disclosure</Link></div>
        <p>Catalog values are planning data until retailer provenance is shown. As an Amazon Associate I earn from qualifying purchases.</p>
      </footer>
    </div>
  );
};

export default Home;
