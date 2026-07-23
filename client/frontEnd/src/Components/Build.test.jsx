import { MemoryRouter, Route, Routes } from "react-router-dom";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import Build from "./Build";
import { SessionProvider } from "../auth/SessionContext";
import api from "../services/api";

vi.mock("../services/api", () => ({
  default: { get: vi.fn(), post: vi.fn(), put: vi.fn() },
}));

const part = (id, name, manufacturer, specifications, price = 10000) => ({
  _id: id,
  name,
  manufacturer,
  specifications,
  price,
});

const catalog = {
  processors: [part("cpu-1", "Ryzen 7", "AMD", { socket: "AM5", cores: 8, threads: 16, tdp: "120W" })],
  motherboards: [part("board-1", "B650 Board", "ASUS", { socket: "AM5", ram_type: "DDR5", form_factor: "ATX" })],
  gpus: [part("gpu-1", "RTX 4070", "NVIDIA", { memory: "12GB", core_count: 5888, tdp: "200W" })],
  storage: [
    part("nvme-1", "1TB NVMe", "Samsung", { interface: "NVMe", capacity: "1TB" }),
    part("sata-1", "2TB SATA", "Crucial", { interface: "SATA", capacity: "2TB" }),
  ],
  ram: [part("ram-1", "32GB DDR5", "Corsair", { type: "DDR5", capacity: "32GB" })],
  powerSupplies: [part("psu-1", "750W Gold", "Corsair", { wattage: "750W", efficiency: "Gold" })],
  cabinets: [part("case-1", "Airflow Case", "NZXT", { form_factor: "ATX", motherboard_support: ["ATX"] })],
};

const compatible = {
  status: "compatible",
  checks: [
    { id: "cpu-socket", label: "CPU socket", status: "pass", message: "AM5 matches." },
    { id: "memory-type", label: "Memory", status: "pass", message: "DDR5 matches." },
    { id: "power-budget", label: "Power", status: "pass", message: "750W is sufficient." },
    { id: "case-form-factor", label: "Case", status: "pass", message: "ATX fits." },
  ],
  power: { recommendedPsu: 550 },
};

describe("Build journey", () => {
  beforeEach(() => {
    localStorage.clear();
    localStorage.setItem("token", "valid-token");
    localStorage.setItem("sessionUser", JSON.stringify({ fullname: "Builder", email: "builder@example.com" }));
    api.get.mockReset();
    api.post.mockReset();
    api.put.mockReset();
    api.get.mockResolvedValue({ data: { data: catalog } });
    api.post.mockImplementation((url) => {
      if (url === "/api/v1/compatibility/evaluate") return Promise.resolve({ data: compatible });
      if (url === "/api/v1/analytics/estimate") {
        return Promise.resolve({
          data: {
            confidence: "low",
            model: { version: "planning-2.0.0" },
            performance: { cpuParallelismIndex: 356, gpuMemoryGB: 12, gpuBoardPowerWatts: 200 },
          },
        });
      }
      if (url === "/saves") return Promise.resolve({ status: 201, data: { _id: "saved-1" } });
      return Promise.reject(new Error(`Unexpected POST ${url}`));
    });
  });

  it("builds, verifies, and saves a catalog-backed configuration", async () => {
    const user = userEvent.setup();
    render(
      <SessionProvider>
        <MemoryRouter initialEntries={["/build"]}>
          <Routes>
            <Route path="/build" element={<Build />} />
            <Route path="/profile" element={<h1>Saved builds</h1>} />
          </Routes>
        </MemoryRouter>
      </SessionProvider>
    );

    const chooseAndContinue = async (name) => {
      const picker = await screen.findByRole("listbox");
      await user.click(within(picker).getByRole("option", { name: new RegExp(name, "i") }));
      await user.click(screen.getByRole("button", { name: /continue/i }));
    };

    await chooseAndContinue("AMD");
    await chooseAndContinue("Ryzen 7");
    await chooseAndContinue("B650 Board");
    await chooseAndContinue("RTX 4070");
    await chooseAndContinue("1TB NVMe");
    await user.click(screen.getByRole("button", { name: "Continue without a secondary drive" }));
    await chooseAndContinue("32GB DDR5");
    await chooseAndContinue("750W Gold");
    await chooseAndContinue("Airflow Case");

    expect(await screen.findByRole("heading", { name: "Server verified" })).toBeInTheDocument();
    expect(await screen.findByText(/CPU index 356 \/ 12 GB GPU memory · low confidence/i)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Save build" }));

    await waitFor(() => expect(api.post).toHaveBeenCalledWith(
      "/saves",
      expect.objectContaining({
        cpu: "Ryzen 7",
        motherboard: "B650 Board",
        gpu: "RTX 4070",
        secondaryStorage: "",
        componentIds: expect.objectContaining({ processor: "cpu-1", cabinet: "case-1" }),
      })
    ));
    expect(await screen.findByRole("heading", { name: "Saved builds" })).toBeInTheDocument();
  });
});
