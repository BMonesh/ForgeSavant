import { MemoryRouter } from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import Profile from "./Profile";
import api from "../services/api";
import { SessionProvider } from "../auth/SessionContext";

vi.mock("../services/api", () => ({
  default: { get: vi.fn(), delete: vi.fn() },
}));

const savedBuild = {
  _id: "build-1",
  cpu: "Ryzen 7 7800X3D",
  motherboard: "B650 Board",
  gpu: "RTX 4070 Super",
  primaryStorage: "1TB NVMe",
  secondaryStorage: "2TB SATA",
  ram: "32GB DDR5",
  powerSupply: "750W Gold",
  cabinet: "Airflow Case",
  analytics: {
    performance: { cpuParallelismIndex: 412, gpuMemoryGB: 12, gpuBoardPowerWatts: 220 },
  },
  image: "case.png",
};

describe("Profile", () => {
  beforeEach(() => {
    localStorage.clear();
    localStorage.setItem("token", "valid-token");
    localStorage.setItem("sessionUser", JSON.stringify({ fullname: "Monesh", email: "test@example.com" }));
    api.get.mockReset();
    api.delete.mockReset();
  });

  it("loads server-scoped builds and confirms deletion", async () => {
    const user = userEvent.setup();
    api.get.mockResolvedValue({ data: [savedBuild] });
    api.delete.mockResolvedValue({ data: { message: "Deleted" } });

    render(<SessionProvider><MemoryRouter><Profile /></MemoryRouter></SessionProvider>);
    expect(await screen.findByRole("heading", { name: savedBuild.cpu })).toBeInTheDocument();
    expect(screen.getByText("412")).toBeInTheDocument();
    expect(screen.getByText("12 GB")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /open build/i })).toHaveAttribute("href", "/build");
    await user.click(screen.getByRole("button", { name: /remove/i }));
    expect(screen.getByRole("alertdialog")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Remove build" }));

    await waitFor(() => expect(api.delete).toHaveBeenCalledWith("/delsaves/build-1"));
    expect(screen.getByText("No saved builds")).toBeInTheDocument();
  });

  it("provides a retry action when loading fails", async () => {
    api.get.mockRejectedValueOnce({ response: { data: { error: "Database unavailable" } } });
    render(<SessionProvider><MemoryRouter><Profile /></MemoryRouter></SessionProvider>);
    expect(await screen.findByText("Database unavailable")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
  });
});
