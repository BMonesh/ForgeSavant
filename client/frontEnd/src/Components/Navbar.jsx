import { useEffect, useRef, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import {
  FiActivity,
  FiBarChart2,
  FiChevronDown,
  FiDatabase,
  FiFolder,
  FiLayers,
  FiLink,
  FiLogIn,
  FiLogOut,
  FiMenu,
  FiTool,
  FiX,
} from "react-icons/fi";
import "../Styles/Navbar.css";
import BrandLogo from "./ui/BrandLogo";
import { useSession } from "../auth/SessionContext";

const operations = [
  { to: "/admin/affiliate-links", label: "Affiliate links", icon: FiLink },
  { to: "/admin/content", label: "Product content", icon: FiLayers },
  { to: "/admin/data-quality", label: "Data health", icon: FiActivity },
  { to: "/admin/offers", label: "Data import", icon: FiDatabase },
];

const Navbar = () => {
  const location = useLocation();
  const { isAuthenticated, signOut, user } = useSession();
  const [operationsOpen, setOperationsOpen] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const operationsButtonRef = useRef(null);
  const mobileButtonRef = useRef(null);
  const operationsRef = useRef(null);
  const isBuildPage = location.pathname === "/build";
  const isOperationsPage = operations.some(({ to }) => location.pathname === to);
  const isActive = (path) => location.pathname === path;

  useEffect(() => {
    setOperationsOpen(false);
    setMobileOpen(false);
  }, [location.pathname, location.hash]);

  useEffect(() => {
    const handleKeyDown = (event) => {
      if (event.key !== "Escape") return;
      if (operationsOpen) {
        setOperationsOpen(false);
        operationsButtonRef.current?.focus();
      } else if (mobileOpen) {
        setMobileOpen(false);
        mobileButtonRef.current?.focus();
      }
    };

    const handlePointerDown = (event) => {
      if (operationsOpen && !operationsRef.current?.contains(event.target)) {
        setOperationsOpen(false);
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    document.addEventListener("pointerdown", handlePointerDown);
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      document.removeEventListener("pointerdown", handlePointerDown);
    };
  }, [mobileOpen, operationsOpen]);

  const handleSignOut = () => {
    setMobileOpen(false);
    signOut();
  };

  return (
    <header className="Navbar">
      <div className="left-Nav">
        <Link to="/" className="brand-link" aria-label="ForgeSavant home">
          <BrandLogo className="nav-brand-logo" decorative />
        </Link>
        {isBuildPage ? (
          <div className="nav-build-name" aria-label="Current build">
            <span>Build</span>
            <strong>Untitled configuration</strong>
          </div>
        ) : null}
      </div>

      <nav className="right-Nav" aria-label="Primary navigation">
        <Link
          to="/#components"
          className="nav-action nav-explore"
          aria-current={location.pathname === "/" && location.hash === "#components" ? "page" : undefined}
        >
          Explore
        </Link>
        <Link
          to="/build"
          className="nav-action"
          aria-label="Open builder"
          aria-current={isBuildPage ? "page" : undefined}
        >
          <FiTool aria-hidden="true" />
          <span>Builder</span>
        </Link>
        <Link to="/benchmarks" className="nav-action nav-benchmarks" aria-current={isActive("/benchmarks") ? "page" : undefined}>
          <FiBarChart2 aria-hidden="true" />
          <span>Benchmarks</span>
        </Link>
        {isAuthenticated ? (
          <Link
            to="/profile"
            className="nav-action"
            aria-current={isActive("/profile") ? "page" : undefined}
          >
            <FiFolder aria-hidden="true" />
            <span>My builds</span>
          </Link>
        ) : (
          <Link
            to="/login"
            className="nav-action"
            aria-current={isActive("/login") ? "page" : undefined}
          >
            <FiLogIn aria-hidden="true" />
            <span>Sign in</span>
          </Link>
        )}

        {user?.isAdmin ? (
          <div className="nav-menu-group nav-operations" ref={operationsRef}>
            <button
              ref={operationsButtonRef}
              type="button"
              className="nav-action nav-menu-trigger"
              aria-expanded={operationsOpen}
              aria-controls="operations-menu"
              aria-current={isOperationsPage ? "page" : undefined}
              onClick={() => setOperationsOpen((open) => !open)}
            >
              <span>Operations</span>
              <FiChevronDown aria-hidden="true" />
            </button>
            {operationsOpen ? (
              <div id="operations-menu" className="nav-dropdown" aria-label="Operations">
                <p>Data platform</p>
                {operations.map(({ to, label, icon: Icon }) => (
                  <Link key={to} to={to} aria-current={isActive(to) ? "page" : undefined}>
                    <Icon aria-hidden="true" />
                    <span>{label}</span>
                  </Link>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}

        {isAuthenticated ? (
          <button type="button" className="nav-action nav-signout" onClick={handleSignOut}>
            <FiLogOut aria-hidden="true" />
            <span>Sign out</span>
          </button>
        ) : null}

        <button
          ref={mobileButtonRef}
          type="button"
          className="nav-mobile-trigger"
          aria-expanded={mobileOpen}
          aria-controls="mobile-navigation"
          onClick={() => setMobileOpen((open) => !open)}
        >
          {mobileOpen ? <FiX aria-hidden="true" /> : <FiMenu aria-hidden="true" />}
          <span>Menu</span>
        </button>
      </nav>

      {mobileOpen ? (
        <nav id="mobile-navigation" className="nav-mobile-panel" aria-label="More navigation">
          <Link to="/#components">Explore components</Link>
          <Link to="/#how-it-works">How it works</Link>
          <Link to="/benchmarks">Benchmark evidence</Link>
          {user?.isAdmin ? (
            <div className="nav-mobile-operations">
              <p>Operations</p>
              {operations.map(({ to, label, icon: Icon }) => (
                <Link key={to} to={to} aria-current={isActive(to) ? "page" : undefined}>
                  <Icon aria-hidden="true" />
                  <span>{label}</span>
                </Link>
              ))}
            </div>
          ) : null}
          {isAuthenticated ? (
            <button type="button" onClick={handleSignOut}>
              <FiLogOut aria-hidden="true" />
              <span>Sign out</span>
            </button>
          ) : null}
        </nav>
      ) : null}
    </header>
  );
};

export default Navbar;
