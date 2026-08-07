import { MemoryRouter, Route, Routes } from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import Login from "./Login";
import api from "../services/api";
import { SessionProvider } from "../auth/SessionContext";

vi.mock("../services/api", () => ({ default: { post: vi.fn() } }));

const renderLogin = () => render(
  <SessionProvider>
    <MemoryRouter initialEntries={["/login"]}>
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route path="/build" element={<h1>Builder route</h1>} />
      </Routes>
    </MemoryRouter>
  </SessionProvider>
);

describe("Login", () => {
  beforeEach(() => {
    localStorage.clear();
    api.post.mockReset();
  });

  it("stores a successful session and returns to the builder", async () => {
    const user = userEvent.setup();
    api.post.mockResolvedValue({ data: { token: "token", user: { fullname: "Monesh", email: "test@example.com" } } });
    renderLogin();
    await user.type(screen.getByLabelText("Email"), "test@example.com");
    await user.type(screen.getByLabelText("Password"), "password123");
    await user.click(screen.getByRole("button", { name: "Sign in" }));

    expect(await screen.findByRole("heading", { name: "Builder route" })).toBeInTheDocument();
    expect(localStorage.getItem("token")).toBe("token");
  });

  it("shows backend authentication errors without leaving the form", async () => {
    const user = userEvent.setup();
    api.post.mockRejectedValue({ response: { data: { error: "Invalid Credentials" } } });
    renderLogin();
    await user.type(screen.getByLabelText("Email"), "test@example.com");
    await user.type(screen.getByLabelText("Password"), "wrong-password");
    await user.click(screen.getByRole("button", { name: "Sign in" }));

    await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent("Invalid Credentials"));
    expect(screen.getByRole("heading", { name: "Sign in" })).toBeInTheDocument();
  });
});
