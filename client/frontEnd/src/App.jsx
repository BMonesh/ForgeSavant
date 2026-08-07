/* eslint-disable react/prop-types */
import { BrowserRouter as Router, Navigate, Routes, Route, useLocation } from "react-router-dom";
import Navbar from "./Components/Navbar";
import Build from "./Components/Build";
import Home from "./Components/Home";
import Signup from "./Components/Signup";
import Login from "./Components/Login";
import Profile from "./Components/Profile";
import AdminOffers from "./Components/AdminOffers";
import AdminDataQuality from "./Components/AdminDataQuality";
import AdminContent from "./Components/AdminContent";
import ComponentDetail from "./Components/ComponentDetail";
import AdminAffiliateLinks from "./Components/AdminAffiliateLinks";
import AffiliateDisclosure from "./Components/AffiliateDisclosure";
import Benchmarks from "./Components/Benchmarks";
import About from "./Components/About";
import NotFound from "./Components/NotFound";
import './App.css'
import { useSession } from "./auth/SessionContext";

const RequireAuth = ({ children }) => {
  const { isAuthenticated } = useSession();
  const location = useLocation();

  return isAuthenticated ? children : (
    <Navigate
      to="/login"
      replace
      state={{ returnTo: location.pathname }}
    />
  );
};

const RequireAdmin = ({ children }) => {
  const { isAuthenticated, user } = useSession();
  const location = useLocation();
  if (!isAuthenticated) return <Navigate to="/login" replace state={{ returnTo: location.pathname }} />;
  return user?.isAdmin ? children : <Navigate to="/profile" replace />;
};

const renderPage = (children, withNav = true) => (
  <>
    {withNav && <Navbar />}
    <main id="main-content" className="app-main">
      {children}
    </main>
  </>
);

const App = () => {
  return (
    <Router future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <a className="skip-link" href="#main-content">Skip to main content</a>
      <Routes>
        <Route path="/" element={renderPage(<Home />)} />
        <Route path="/build" element={renderPage(<Build />)} />
        <Route path="/about" element={renderPage(<About />)} />
        <Route path="/login" element={renderPage(<Login />, false)} />
        <Route path="/signup" element={renderPage(<Signup />, false)} />
        <Route path="/loginAuthentication" element={<Navigate to="/login" replace />} />
        <Route path="/SignupAuthentication" element={<Navigate to="/signup" replace />} />
        <Route path="/profile" element={renderPage(<RequireAuth><Profile /></RequireAuth>)} />
        <Route path="/admin/offers" element={renderPage(<RequireAdmin><AdminOffers /></RequireAdmin>)} />
        <Route path="/admin/data-quality" element={renderPage(<RequireAdmin><AdminDataQuality /></RequireAdmin>)} />
        <Route path="/admin/content" element={renderPage(<RequireAdmin><AdminContent /></RequireAdmin>)} />
        <Route path="/admin/affiliate-links" element={renderPage(<RequireAdmin><AdminAffiliateLinks /></RequireAdmin>)} />
        <Route path="/components/:category/:id" element={renderPage(<ComponentDetail />)} />
        <Route path="/affiliate-disclosure" element={renderPage(<AffiliateDisclosure />)} />
        <Route path="/benchmarks" element={renderPage(<Benchmarks />)} />
        <Route path="*" element={renderPage(<NotFound />)} />
      </Routes>
    </Router>
  );
};

export default App;
