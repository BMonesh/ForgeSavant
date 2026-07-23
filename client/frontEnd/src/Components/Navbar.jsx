import { Link, useLocation } from 'react-router-dom';
import { FiActivity, FiDatabase, FiFolder, FiLayers, FiLogIn, FiLogOut, FiTool, FiUser } from 'react-icons/fi';
import '../Styles/Navbar.css';
import BrandLogo from './ui/BrandLogo';
import { useSession } from '../auth/SessionContext';

const Navbar = () => {
  const location = useLocation();
  const isBuildPage = location.pathname === '/build';
  const isHomePage = location.pathname === '/' || location.pathname === '/about';
  const { isAuthenticated, signOut, user } = useSession();
  const isActive = (path) => location.pathname === path;

  return (
    <header className={`Navbar ${isHomePage ? 'Navbar-home' : 'Navbar-workbench'}`}>
      <div className="left-Nav">
        <Link to="/" className="brand-link" aria-label="ForgeSavant home">
          <BrandLogo className="nav-brand-logo" />
        </Link>
        {isBuildPage ? (
          <div className="nav-build-name" aria-label="Current build">
            <span>Build</span>
            <strong>Untitled configuration</strong>
          </div>
        ) : null}
      </div>
      <nav className="right-Nav" aria-label="Primary navigation">
        {isHomePage ? (
          <div className="nav-home-links">
            <Link to="/#recommended">Recommended</Link>
            <Link to="/#components">Components</Link>
            <Link to="/#how-it-works">How it works</Link>
          </div>
        ) : null}
        <Link
          to="/build"
          className="nav-action"
          aria-label="Open builder"
          title="Builder"
          aria-current={isBuildPage ? 'page' : undefined}
        >
          <FiTool aria-hidden="true" />
          <span>Builder</span>
        </Link>
        {isAuthenticated ? (
          <Link
            to="/profile"
            className="nav-action"
            aria-label="My builds"
            aria-current={isActive('/profile') ? 'page' : undefined}
          >
            <FiFolder aria-hidden="true" />
            <span>My builds</span>
          </Link>
        ) : (
          <Link
            to="/loginAuthentication"
            className="nav-action"
            aria-label="Sign in"
            aria-current={isActive('/loginAuthentication') ? 'page' : undefined}
          >
            <FiLogIn aria-hidden="true" />
            <span>Sign in</span>
          </Link>
        )}
        {user?.isAdmin ? (
          <Link
            to="/admin/content"
            className="nav-action"
            aria-label="Product content review"
            aria-current={isActive('/admin/content') ? 'page' : undefined}
          >
            <FiLayers aria-hidden="true" />
            <span>Content</span>
          </Link>
        ) : null}
        {user?.isAdmin ? (
          <Link
            to="/admin/data-quality"
            className="nav-action"
            aria-label="Data quality dashboard"
            aria-current={isActive('/admin/data-quality') ? 'page' : undefined}
          >
            <FiActivity aria-hidden="true" />
            <span>Data health</span>
          </Link>
        ) : null}
        {user?.isAdmin ? (
          <Link
            to="/admin/offers"
            className="nav-action"
            aria-label="Catalog data import"
            aria-current={isActive('/admin/offers') ? 'page' : undefined}
          >
            <FiDatabase aria-hidden="true" />
            <span>Data import</span>
          </Link>
        ) : null}
        {isAuthenticated ? (
          <button type="button" className="nav-logout" onClick={signOut} title="Sign out">
            <FiLogOut aria-hidden="true" />
            <span>Sign out</span>
          </button>
        ) : null}
        {isAuthenticated ? <span className="nav-avatar" title={user?.fullname || "Account"}><FiUser aria-hidden="true" /></span> : null}
      </nav>
    </header>
  );
};

export default Navbar;
