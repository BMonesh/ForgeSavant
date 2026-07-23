import React from 'react'
import ReactDOM from 'react-dom/client'
import { GoogleOAuthProvider } from '@react-oauth/google';
import App from './App.jsx'
import { SessionProvider } from './auth/SessionContext.jsx';

const googleClientId = import.meta.env.VITE_GOOGLE_CLIENT_ID || "";

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <GoogleOAuthProvider clientId={googleClientId}>
      <SessionProvider>
        <App />
      </SessionProvider>
    </GoogleOAuthProvider>
  </React.StrictMode>,
)
