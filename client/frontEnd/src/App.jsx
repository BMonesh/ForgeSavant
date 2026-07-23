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
import './App.css'
import { useSession } from "./auth/SessionContext";

const RequireAuth = ({ children }) => {
  const { isAuthenticated } = useSession();
  const location = useLocation();

  return isAuthenticated ? children : (
    <Navigate
      to="/loginAuthentication"
      replace
      state={{ returnTo: location.pathname }}
    />
  );
};

const RequireAdmin = ({ children }) => {
  const { isAuthenticated, user } = useSession();
  const location = useLocation();
  if (!isAuthenticated) return <Navigate to="/loginAuthentication" replace state={{ returnTo: location.pathname }} />;
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
    <Router>
      <a className="skip-link" href="#main-content">Skip to main content</a>
      <Routes>
        <Route path="/" element={renderPage(<Home />)} />
        <Route path="/build" element={renderPage(<Build />)} />
        <Route path="/about" element={renderPage(<Home />)} />
        <Route path="/loginAuthentication" element={renderPage(<Login />, false)} />
        <Route path="/SignupAuthentication" element={renderPage(<Signup />, false)} />
        <Route path="/signup" element={renderPage(<Signup />, false)} />
        <Route path="/profile" element={renderPage(<RequireAuth><Profile /></RequireAuth>)} />
        <Route path="/admin/offers" element={renderPage(<RequireAdmin><AdminOffers /></RequireAdmin>)} />
        <Route path="/admin/data-quality" element={renderPage(<RequireAdmin><AdminDataQuality /></RequireAdmin>)} />
        <Route path="/admin/content" element={renderPage(<RequireAdmin><AdminContent /></RequireAdmin>)} />
        <Route path="/components/:category/:id" element={renderPage(<ComponentDetail />)} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Router>
  );
};

export default App;
