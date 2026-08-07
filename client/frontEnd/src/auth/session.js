const SESSION_EVENT = "forgesavant:session-changed";

const readUser = () => {
  const stored = localStorage.getItem("sessionUser");

  if (stored) {
    try {
      return JSON.parse(stored);
    } catch {
      localStorage.removeItem("sessionUser");
    }
  }

  const fullname = localStorage.getItem("user");
  const email = localStorage.getItem("email");
  return fullname || email ? { fullname, email } : null;
};

export const getSession = () => ({
  token: localStorage.getItem("token"),
  user: readUser(),
});

export const saveSession = ({ token, user }) => {
  localStorage.setItem("token", token);
  localStorage.setItem("sessionUser", JSON.stringify(user));
  localStorage.setItem("user", user.fullname);
  localStorage.setItem("email", user.email);
  window.dispatchEvent(new Event(SESSION_EVENT));
};

export const clearSession = () => {
  ["token", "sessionUser", "user", "email"].forEach((key) =>
    localStorage.removeItem(key)
  );
  window.dispatchEvent(new Event(SESSION_EVENT));
};

export const subscribeToSession = (listener) => {
  window.addEventListener(SESSION_EVENT, listener);
  window.addEventListener("storage", listener);

  return () => {
    window.removeEventListener(SESSION_EVENT, listener);
    window.removeEventListener("storage", listener);
  };
};
