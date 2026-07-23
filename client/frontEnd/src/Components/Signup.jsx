import { useState } from "react";
import { GoogleLogin } from "@react-oauth/google";
import { Link, useLocation, useNavigate } from "react-router-dom";
import "../Styles/signup.css";
import api from "../services/api";
import { useSession } from "../auth/SessionContext";
import { getAuthError } from "../auth/authErrors";
import BrandLogo from "./ui/BrandLogo";

function Signup() {
  const [fullname, setFullname] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [message, setMessage] = useState("");
  const [emailError, setEmailError] = useState("");
  const [passwordError, setPasswordError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const { signIn } = useSession();
  const isGoogleAuthEnabled = Boolean(import.meta.env.VITE_GOOGLE_CLIENT_ID);

  const validateForm = () => {
    const nextEmailError = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
      ? ""
      : "Enter a valid email address.";
    const nextPasswordError = password.length >= 8
      ? ""
      : "Use at least 8 characters.";
    setEmailError(nextEmailError);
    setPasswordError(nextPasswordError);
    return !nextEmailError && !nextPasswordError;
  };

  const handleFormSubmit = async (event) => {
    event.preventDefault();
    setMessage("");
    if (!validateForm()) return;
    setIsSubmitting(true);

    try {
      const response = await api.post("/signup", { fullname, email, password });
      signIn(response.data);
      navigate(location.state?.returnTo || "/build", { replace: true });
    } catch (error) {
      console.error("Error during sign-up:", error);
      setMessage(getAuthError(error, "Account creation failed. Try again."));
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleGoogleSignUp = async (credentialResponse) => {
    setMessage("");
    setIsSubmitting(true);

    try {
      const response = await api.post("/googleSignup", {
        credential: credentialResponse.credential,
      });
      signIn(response.data);
      navigate(location.state?.returnTo || "/build", { replace: true });
    } catch (error) {
      console.error("Error during Google sign-up:", error);
      setMessage(getAuthError(error, "Google account creation failed. Try again."));
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="Signup">
      <section className="auth-context" aria-label="ForgeSavant account benefits">
        <Link to="/" className="auth-brand" aria-label="ForgeSavant builder">
          <BrandLogo className="brand-logo-auth" />
        </Link>
        <div className="auth-copy">
          <p className="ui-kicker">New builder profile</p>
          <h1>Start with a clean parts record.</h1>
          <p>Store each verified selection against one account and return to the complete build later.</p>
        </div>
        <dl className="auth-proof">
          <div><dt>Verified flow</dt><dd>Selections unlock when upstream rules match.</dd></div>
          <div><dt>Private history</dt><dd>Saved builds are protected by your session.</dd></div>
        </dl>
      </section>

      <main className="auth-form-panel">
        <div className="auth-form-shell">
          <header className="head">
            <p className="ui-kicker">Create account</p>
            <h2>Builder profile</h2>
            <p className="signup-text">Save compatible configurations and revisit them from any session.</p>
          </header>
          <form onSubmit={handleFormSubmit} className="form">
            <label htmlFor="signup-fullname">Full name</label>
            <input id="signup-fullname" type="text" autoComplete="name" placeholder="Your name" value={fullname} onChange={(event) => setFullname(event.target.value)} required />
            <label htmlFor="signup-email">Email</label>
            <input id="signup-email" type="email" autoComplete="email" placeholder="you@example.com" aria-describedby={emailError ? "signup-email-error" : undefined} value={email} onChange={(event) => setEmail(event.target.value)} required />
            {emailError ? <p id="signup-email-error" className="field-error" role="alert">{emailError}</p> : null}
            <label htmlFor="signup-password">Password</label>
            <input id="signup-password" type="password" autoComplete="new-password" placeholder="At least 8 characters" aria-describedby={passwordError ? "signup-password-error" : undefined} value={password} onChange={(event) => setPassword(event.target.value)} required />
            {passwordError ? <p id="signup-password-error" className="field-error" role="alert">{passwordError}</p> : null}
            <button type="submit" className="register" disabled={isSubmitting}>
              {isSubmitting ? "Creating account..." : "Create account"}
            </button>
            {isGoogleAuthEnabled ? <GoogleLogin onSuccess={handleGoogleSignUp} onError={() => setMessage("Google account creation failed. Try again.")} /> : null}
            <Link to="/loginAuthentication" state={location.state} className="auth-link">Sign in instead</Link>
          </form>
          {message ? <p className="error-message" role="alert">{message}</p> : null}
        </div>
      </main>
    </div>
  );
}

export default Signup;
