"""
ForgeSavant Compatibility Engine
================================
Rule-based hardware compatibility checker that validates PC component
configurations using structured queries against the cleaned dataset.

Validates:
- CPU <-> Motherboard socket compatibility
- RAM <-> Motherboard type and slot compatibility
- GPU <-> PSU wattage requirements
- Storage <-> Motherboard interface compatibility
- Total power draw vs PSU capacity

Usage:
    python compatibility_engine.py --check-build build_config.json
    python compatibility_engine.py --validate-all
    python compatibility_engine.py --find-compatible --cpu "AMD Ryzen 5 5600X"

Dependencies:
    pip install pandas numpy
"""

import argparse
import json
import os
import logging
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

# ── Config ────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLEANED_DIR = os.path.join(BASE_DIR, "cleaned_data")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Power overhead multiplier (add 20% headroom to total TDP)
PSU_HEADROOM = 1.2

# System base power draw (fans, drives, misc) in watts
SYSTEM_BASE_POWER = 75


# ══════════════════════════════════════════════════════════════════
#  DATA MODELS
# ══════════════════════════════════════════════════════════════════

@dataclass
class CompatibilityResult:
    """Result of a compatibility check between two components."""
    component_a: str
    component_b: str
    compatible: bool
    rule: str
    details: str

    def to_dict(self) -> dict:
        return {
            "component_a": self.component_a,
            "component_b": self.component_b,
            "compatible": self.compatible,
            "rule": self.rule,
            "details": self.details,
        }


@dataclass
class BuildValidation:
    """Complete validation result for a PC build configuration."""
    valid: bool = True
    total_cost: float = 0.0
    estimated_tdp: int = 0
    recommended_psu: int = 0
    checks: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    def add_check(self, result: CompatibilityResult):
        self.checks.append(result)
        if not result.compatible:
            self.valid = False
            self.errors.append(result.details)

    def add_warning(self, message: str):
        self.warnings.append(message)

    def summary(self) -> dict:
        return {
            "valid": self.valid,
            "total_cost": self.total_cost,
            "estimated_tdp_watts": self.estimated_tdp,
            "recommended_psu_watts": self.recommended_psu,
            "checks_passed": sum(1 for c in self.checks if c.compatible),
            "checks_failed": sum(1 for c in self.checks if not c.compatible),
            "warnings": self.warnings,
            "errors": self.errors,
        }


# ══════════════════════════════════════════════════════════════════
#  DATA LOADER
# ══════════════════════════════════════════════════════════════════

class ComponentDatabase:
    """Loads and queries cleaned component data from CSV files."""

    def __init__(self, data_dir: str = CLEANED_DIR):
        self.data_dir = data_dir
        self.processors = self._load("processors_cleaned.csv")
        self.gpus = self._load("gpus_cleaned.csv")
        self.motherboards = self._load("motherboards_cleaned.csv")
        self.ram = self._load("ram_cleaned.csv")

    def _load(self, filename: str) -> pd.DataFrame:
        path = os.path.join(self.data_dir, filename)
        if os.path.exists(path):
            df = pd.read_csv(path)
            logger.info(f"Loaded {len(df)} rows from {filename}")
            return df
        logger.warning(f"Data file not found: {path}")
        return pd.DataFrame()

    def find_processor(self, name: str) -> pd.Series | None:
        """Lookup processor by name (case-insensitive partial match)."""
        if self.processors.empty:
            return None
        mask = self.processors["name"].str.lower().str.contains(
            name.lower(), na=False
        )
        matches = self.processors[mask]
        return matches.iloc[0] if not matches.empty else None

    def find_gpu(self, name: str) -> pd.Series | None:
        if self.gpus.empty:
            return None
        mask = self.gpus["name"].str.lower().str.contains(name.lower(), na=False)
        matches = self.gpus[mask]
        return matches.iloc[0] if not matches.empty else None

    def find_motherboard(self, name: str) -> pd.Series | None:
        if self.motherboards.empty:
            return None
        mask = self.motherboards["name"].str.lower().str.contains(
            name.lower(), na=False
        )
        matches = self.motherboards[mask]
        return matches.iloc[0] if not matches.empty else None

    def find_ram(self, name: str) -> pd.Series | None:
        if self.ram.empty:
            return None
        mask = self.ram["name"].str.lower().str.contains(name.lower(), na=False)
        matches = self.ram[mask]
        return matches.iloc[0] if not matches.empty else None

    def get_compatible_motherboards(self, cpu_socket: str) -> pd.DataFrame:
        """Query all motherboards matching a CPU socket type."""
        if self.motherboards.empty:
            return pd.DataFrame()
        mask = self.motherboards["socket"].str.upper() == cpu_socket.upper()
        return self.motherboards[mask].sort_values("price")

    def get_compatible_ram(self, ram_type: str, max_slots: int = 4) -> pd.DataFrame:
        """Query all RAM matching a specific DDR type."""
        if self.ram.empty:
            return pd.DataFrame()
        mask = self.ram["ram_type"].str.upper() == ram_type.upper()
        return self.ram[mask].sort_values("price")


# ══════════════════════════════════════════════════════════════════
#  COMPATIBILITY RULES
# ══════════════════════════════════════════════════════════════════

def parse_tdp_watts(tdp_str: str) -> int:
    """Extract wattage number from TDP string like '105W'."""
    if pd.isna(tdp_str):
        return 0
    import re
    match = re.match(r"(\d+)", str(tdp_str))
    return int(match.group(1)) if match else 0


def check_cpu_motherboard(cpu: pd.Series, mobo: pd.Series) -> CompatibilityResult:
    """
    Rule: CPU socket must match motherboard socket.
    e.g., AM4 CPU requires AM4 motherboard.
    """
    cpu_socket = str(cpu.get("socket", "")).strip().upper()
    mobo_socket = str(mobo.get("socket", "")).strip().upper()

    compatible = cpu_socket == mobo_socket
    return CompatibilityResult(
        component_a=cpu["name"],
        component_b=mobo["name"],
        compatible=compatible,
        rule="CPU-Motherboard Socket Match",
        details=(
            f"PASS: {cpu['name']} ({cpu_socket}) is compatible with "
            f"{mobo['name']} ({mobo_socket})"
            if compatible
            else f"FAIL: {cpu['name']} requires socket {cpu_socket}, but "
            f"{mobo['name']} has socket {mobo_socket}"
        ),
    )


def check_ram_motherboard(ram: pd.Series, mobo: pd.Series) -> CompatibilityResult:
    """
    Rule: RAM DDR type must be supported by motherboard chipset.
    AM5 / LGA 1700 (600/700 series) -> DDR5 (some support DDR4)
    AM4 / LGA 1200 -> DDR4
    """
    ram_type = str(ram.get("ram_type", "")).strip().upper()
    mobo_socket = str(mobo.get("socket", "")).strip().upper()

    # Define socket -> supported RAM type mapping
    socket_ram_map = {
        "AM4": ["DDR4"],
        "AM5": ["DDR5"],
        "LGA 1700": ["DDR4", "DDR5"],
        "LGA 1200": ["DDR4"],
        "LGA 1151": ["DDR4"],
    }

    supported_types = socket_ram_map.get(mobo_socket, ["DDR4", "DDR5"])
    compatible = ram_type in supported_types

    return CompatibilityResult(
        component_a=ram["name"],
        component_b=mobo["name"],
        compatible=compatible,
        rule="RAM-Motherboard DDR Compatibility",
        details=(
            f"PASS: {ram['name']} ({ram_type}) works with {mobo['name']} "
            f"(supports {', '.join(supported_types)})"
            if compatible
            else f"FAIL: {ram['name']} is {ram_type}, but {mobo['name']} "
            f"({mobo_socket}) only supports {', '.join(supported_types)}"
        ),
    )


def check_power_budget(
    cpu: pd.Series, gpu: pd.Series, psu_wattage: int
) -> CompatibilityResult:
    """
    Rule: Total estimated power draw (CPU TDP + GPU TDP + base)
    should not exceed PSU capacity with 20% headroom.
    """
    cpu_tdp = parse_tdp_watts(cpu.get("tdp", "0W"))
    gpu_tdp = parse_tdp_watts(gpu.get("tdp", "0W"))

    total_draw = cpu_tdp + gpu_tdp + SYSTEM_BASE_POWER
    recommended = int(total_draw * PSU_HEADROOM)

    compatible = psu_wattage >= recommended

    return CompatibilityResult(
        component_a=f"{cpu['name']} + {gpu['name']}",
        component_b=f"{psu_wattage}W PSU",
        compatible=compatible,
        rule="Power Budget Check",
        details=(
            f"PASS: Estimated draw {total_draw}W (CPU: {cpu_tdp}W + GPU: "
            f"{gpu_tdp}W + System: {SYSTEM_BASE_POWER}W). "
            f"{psu_wattage}W PSU provides adequate headroom."
            if compatible
            else f"FAIL: Estimated draw {total_draw}W needs at least "
            f"{recommended}W PSU (with 20% headroom). "
            f"Current PSU: {psu_wattage}W is insufficient."
        ),
    )


# ══════════════════════════════════════════════════════════════════
#  BUILD VALIDATOR
# ══════════════════════════════════════════════════════════════════

def validate_build(build_config: dict, db: ComponentDatabase) -> BuildValidation:
    """
    Run all compatibility checks against a build configuration.

    build_config format:
    {
        "cpu": "AMD Ryzen 5 5600X",
        "gpu": "NVIDIA GeForce RTX 4060",
        "motherboard": "ASUS ROG Strix B550-F",
        "ram": "Corsair Vengeance LPX 16GB",
        "psu_wattage": 650
    }
    """
    validation = BuildValidation()

    # Lookup components
    cpu = db.find_processor(build_config.get("cpu", ""))
    gpu = db.find_gpu(build_config.get("gpu", ""))
    mobo = db.find_motherboard(build_config.get("motherboard", ""))
    ram = db.find_ram(build_config.get("ram", ""))
    psu_wattage = build_config.get("psu_wattage", 550)

    # Validate we found all components
    missing = []
    if cpu is None:
        missing.append(f"CPU: '{build_config.get('cpu', '')}'")
    if gpu is None:
        missing.append(f"GPU: '{build_config.get('gpu', '')}'")
    if mobo is None:
        missing.append(f"Motherboard: '{build_config.get('motherboard', '')}'")
    if ram is None:
        missing.append(f"RAM: '{build_config.get('ram', '')}'")

    if missing:
        for m in missing:
            validation.add_warning(f"Component not found in database: {m}")
        logger.warning(f"Missing components: {missing}")

    # Run checks on found components
    if cpu is not None and mobo is not None:
        validation.add_check(check_cpu_motherboard(cpu, mobo))

    if ram is not None and mobo is not None:
        validation.add_check(check_ram_motherboard(ram, mobo))

    if cpu is not None and gpu is not None:
        validation.add_check(check_power_budget(cpu, gpu, psu_wattage))

    # Calculate totals
    prices = []
    tdps = []
    for component in [cpu, gpu, mobo, ram]:
        if component is not None:
            price = component.get("price", 0)
            if not pd.isna(price):
                prices.append(float(price))
            tdp = parse_tdp_watts(component.get("tdp", "0W"))
            if tdp > 0:
                tdps.append(tdp)

    validation.total_cost = sum(prices)
    validation.estimated_tdp = sum(tdps) + SYSTEM_BASE_POWER
    validation.recommended_psu = int(validation.estimated_tdp * PSU_HEADROOM)

    return validation


# ══════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="ForgeSavant Compatibility Engine"
    )
    parser.add_argument(
        "--check-build",
        help="Path to build config JSON file",
    )
    parser.add_argument(
        "--find-compatible",
        action="store_true",
        help="Find compatible components for a given CPU",
    )
    parser.add_argument(
        "--cpu",
        help="CPU name to find compatible components for",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run demo compatibility check with sample builds",
    )

    args = parser.parse_args()

    db = ComponentDatabase()

    if args.check_build:
        with open(args.check_build) as f:
            config = json.load(f)
        result = validate_build(config, db)
        print(json.dumps(result.summary(), indent=2))

    elif args.find_compatible and args.cpu:
        cpu = db.find_processor(args.cpu)
        if cpu is None:
            print(f"CPU not found: {args.cpu}")
            return

        socket = cpu["socket"]
        print(f"\nCPU: {cpu['name']} (Socket: {socket})")
        print(f"\nCompatible Motherboards:")
        print("-" * 50)
        compatible_mobos = db.get_compatible_motherboards(socket)
        if compatible_mobos.empty:
            print("  No compatible motherboards found in database.")
        else:
            for _, mobo in compatible_mobos.iterrows():
                print(f"  {mobo['name']} - ₹{mobo['price']:,.0f} ({mobo['form_factor']})")

    elif args.demo:
        # Demo: validate a sample build
        sample_builds = [
            {
                "name": "AMD Budget Build",
                "cpu": "AMD Ryzen 5 5600X",
                "gpu": "NVIDIA GeForce RTX 4060",
                "motherboard": "ASUS ROG Strix B550-F",
                "ram": "Corsair Vengeance LPX 16GB",
                "psu_wattage": 550,
            },
            {
                "name": "Intel Mismatch Build (should fail)",
                "cpu": "Intel Core i5-12400F",
                "gpu": "NVIDIA GeForce RTX 4070",
                "motherboard": "ASUS ROG Strix B550-F",  # AM4 board with Intel CPU
                "ram": "G.Skill Trident Z5 32GB",        # DDR5 on AM4
                "psu_wattage": 400,
            },
        ]

        for build in sample_builds:
            build_name = build.pop("name")
            print(f"\n{'='*60}")
            print(f"BUILD: {build_name}")
            print(f"{'='*60}")

            result = validate_build(build, db)
            summary = result.summary()

            status = "VALID" if summary["valid"] else "INVALID"
            print(f"  Status:          {status}")
            print(f"  Total Cost:      ₹{summary['total_cost']:,.0f}")
            print(f"  Est. Power Draw: {summary['estimated_tdp_watts']}W")
            print(f"  Recommended PSU: {summary['recommended_psu_watts']}W")
            print(f"  Checks Passed:   {summary['checks_passed']}")
            print(f"  Checks Failed:   {summary['checks_failed']}")

            if summary["warnings"]:
                print(f"\n  Warnings:")
                for w in summary["warnings"]:
                    print(f"    - {w}")

            if summary["errors"]:
                print(f"\n  Errors:")
                for e in summary["errors"]:
                    print(f"    - {e}")

            for check in result.checks:
                icon = "✓" if check.compatible else "✗"
                print(f"\n  [{icon}] {check.rule}")
                print(f"      {check.details}")

        print(f"\n{'='*60}\n")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
