import { useState } from "react";
import { GoogleLogin } from "@react-oauth/google";
import { Link, useLocation, useNavigate } from "react-router-dom";
import "../Styles/login.css";
import api from "../services/api";
import { useSession } from "../auth/SessionContext";
import { getAuthError } from "../auth/authErrors";
import BrandLogo from "./ui/BrandLogo";

function Login() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [message, setMessage] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const { signIn } = useSession();
  const isGoogleAuthEnabled = Boolean(import.meta.env.VITE_GOOGLE_CLIENT_ID);
  const returnTo = location.state?.returnTo || "/build";

  const handleFormSubmit = async (event) => {
    event.preventDefault();
    setMessage("");
    setIsSubmitting(true);

    try {
      const response = await api.post("/login", { email, password });
      signIn(response.data);
      navigate(returnTo, { replace: true });
    } catch (error) {
      console.error("Error during login:", error);
      setMessage(getAuthError(error, "Sign in failed. Try again."));
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleGoogleLogin = async (credentialResponse) => {
    setMessage("");
    setIsSubmitting(true);

    try {
      const response = await api.post("/googleLogin", {
        credential: credentialResponse.credential,
      });
      signIn(response.data);
      navigate(returnTo, { replace: true });
    } catch (error) {
      console.error("Error during Google login:", error);
      setMessage(getAuthError(error, "Google sign in failed. Try again."));
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="Login">
      <section className="auth-form-panel" aria-labelledby="login-title">
        <div className="auth-form-shell">
          <Link to="/" className="auth-brand" aria-label="ForgeSavant home">
            <BrandLogo className="brand-logo-auth" />
          </Link>
          <header className="head-login">
            <p className="ui-kicker">Account access / saved work</p>
            <h1 id="login-title">Sign in</h1>
            <p className="login-text">Continue to your saved builds and active workbench.</p>
          </header>
          <form onSubmit={handleFormSubmit} className="form-login">
            <label htmlFor="login-email">Email</label>
            <input id="login-email" type="email" autoComplete="email" placeholder="you@example.com" value={email} onChange={(event) => setEmail(event.target.value)} required />
            <label htmlFor="login-password">Password</label>
            <input id="login-password" type="password" autoComplete="current-password" placeholder="Enter your password" value={password} onChange={(event) => setPassword(event.target.value)} required />
            <button type="submit" className="login-button" disabled={isSubmitting}>
              {isSubmitting ? "Signing in..." : "Sign in"}
            </button>
            {isGoogleAuthEnabled ? <div className="auth-provider"><span>Or continue with</span><GoogleLogin onSuccess={handleGoogleLogin} onError={() => setMessage("Google sign in failed. Try again.")} /></div> : null}
            <Link to="/signup" state={location.state} className="auth-link">Create an account</Link>
          </form>
          <div className="auth-status" aria-live="polite">
            {message ? <p className="error-message-login" role="alert">{message}</p> : null}
          </div>
        </div>
      </section>
    </div>
  );
}

export default Login;
