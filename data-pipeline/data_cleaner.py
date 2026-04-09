"""
ForgeSavant Data Cleaner & Normalizer
=====================================
Processes raw CSV data scraped from multiple vendor sources, handling:
- Inconsistent casing and formatting (e.g., "amd" vs "AMD", "ghz" vs "GHz")
- Duplicate entries across sources (keeps lowest price)
- Missing/null value handling
- Unit standardization (clock speeds, memory, TDP, voltage)
- Schema normalization to match MongoDB models

Usage:
    python data_cleaner.py --component processors
    python data_cleaner.py --component gpus
    python data_cleaner.py --all
    python data_cleaner.py --all --stats

Dependencies:
    pip install pandas numpy
"""

import argparse
import os
import re
import json
import logging
from datetime import datetime

import pandas as pd
import numpy as np

# ── Config ────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "raw_data")
CLEANED_DIR = os.path.join(BASE_DIR, "cleaned_data")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Canonical manufacturer names (handles inconsistent casing from sources)
MANUFACTURER_MAP = {
    "amd": "AMD",
    "intel": "Intel",
    "nvidia": "NVIDIA",
    "asus": "ASUS",
    "msi": "MSI",
    "gigabyte": "Gigabyte",
    "asrock": "ASRock",
    "corsair": "Corsair",
    "g.skill": "G.Skill",
    "kingston": "Kingston",
    "teamgroup": "TeamGroup",
    "crucial": "Crucial",
    "samsung": "Samsung",
    "western digital": "Western Digital",
    "seagate": "Seagate",
    "cooler master": "Cooler Master",
}


# ══════════════════════════════════════════════════════════════════
#  GENERIC CLEANING UTILITIES
# ══════════════════════════════════════════════════════════════════

def normalize_manufacturer(name: str) -> str:
    """Standardize manufacturer names to canonical form."""
    if pd.isna(name):
        return "Unknown"
    key = name.strip().lower()
    return MANUFACTURER_MAP.get(key, name.strip().title())


def normalize_clock_speed(value: str) -> str:
    """
    Standardize clock speed formats:
    - '3.7 ghz' -> '3.7 GHz'
    - '3.7GHz'  -> '3.7 GHz'
    - '2460 MHz' -> '2460 MHz'
    - '2460 mhz' -> '2460 MHz'
    """
    if pd.isna(value):
        return ""
    value = str(value).strip()

    # Match number + optional space + unit
    match = re.match(r"([\d.]+)\s*(ghz|mhz)", value, re.IGNORECASE)
    if match:
        num = match.group(1)
        unit = match.group(2).upper()
        # Normalize unit capitalization
        if unit == "GHZ":
            unit = "GHz"
        elif unit == "MHZ":
            unit = "MHz"
        return f"{num} {unit}"
    return value


def normalize_wattage(value: str) -> str:
    """
    Standardize TDP/wattage:
    - '65W' -> '65W'
    - '65 W' -> '65W'
    - '105 w' -> '105W'
    """
    if pd.isna(value):
        return ""
    value = str(value).strip()
    match = re.match(r"(\d+)\s*[wW]", value)
    if match:
        return f"{match.group(1)}W"
    return value


def normalize_memory(value: str) -> str:
    """
    Standardize memory formats:
    - '128 GB' -> '128GB'
    - '8GB GDDR6' -> '8GB GDDR6'
    - '12 GB GDDR6X' -> '12GB GDDR6X'
    """
    if pd.isna(value):
        return ""
    value = str(value).strip()
    # Normalize the number+GB part
    value = re.sub(r"(\d+)\s+(GB|TB|MB)", r"\1\2", value, flags=re.IGNORECASE)
    return value


def normalize_socket(value: str) -> str:
    """
    Standardize CPU socket names:
    - 'lga 1700' -> 'LGA 1700'
    - 'LGA1700' -> 'LGA 1700'
    - 'AM4' stays 'AM4'
    """
    if pd.isna(value):
        return ""
    value = str(value).strip().upper()
    # Add space after LGA if missing
    value = re.sub(r"LGA\s*(\d+)", r"LGA \1", value)
    return value


def normalize_form_factor(value: str) -> str:
    """
    Standardize motherboard form factors:
    - 'atx' -> 'ATX'
    - 'micro-atx' -> 'Micro-ATX'
    - 'mini-itx' -> 'Mini-ITX'
    """
    if pd.isna(value):
        return ""
    value = str(value).strip().lower()
    form_map = {
        "atx": "ATX",
        "micro-atx": "Micro-ATX",
        "mini-itx": "Mini-ITX",
        "e-atx": "E-ATX",
        "extended atx": "E-ATX",
    }
    return form_map.get(value, value.upper())


def normalize_voltage(value: str) -> str:
    """Standardize voltage: '1.35 V' -> '1.35V'"""
    if pd.isna(value):
        return ""
    value = str(value).strip()
    match = re.match(r"([\d.]+)\s*[vV]", value)
    if match:
        return f"{match.group(1)}V"
    return value


def normalize_ram_type(value: str) -> str:
    """Standardize RAM type: 'ddr4' -> 'DDR4', 'DDR5' stays."""
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def deduplicate_by_price(df: pd.DataFrame, name_col: str = "name") -> pd.DataFrame:
    """
    Remove duplicate entries (same component from multiple sources).
    Keeps the entry with the lowest price for each unique component name.
    """
    before_count = len(df)

    # Normalize names for comparison (lowercase, strip whitespace)
    df["_name_key"] = df[name_col].str.strip().str.lower()

    # Sort by price ascending so first occurrence = cheapest
    df = df.sort_values("price", ascending=True)

    # Drop duplicates, keeping first (cheapest)
    df = df.drop_duplicates(subset="_name_key", keep="first")
    df = df.drop(columns=["_name_key"])

    after_count = len(df)
    removed = before_count - after_count
    if removed > 0:
        logger.info(
            f"Deduplication: removed {removed} duplicates "
            f"({before_count} -> {after_count} rows)"
        )

    return df.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════
#  COMPONENT-SPECIFIC CLEANERS
# ══════════════════════════════════════════════════════════════════

def clean_processors(input_path: str, output_path: str) -> pd.DataFrame:
    """Clean and normalize processor data."""
    logger.info(f"Cleaning processors: {input_path}")
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    original_count = len(df)

    # Strip whitespace from all string columns
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

    # Normalize fields
    df["manufacturer"] = df["manufacturer"].apply(normalize_manufacturer)
    df["base_clock"] = df["base_clock"].apply(normalize_clock_speed)
    df["boost_clock"] = df["boost_clock"].apply(normalize_clock_speed)
    df["tdp"] = df["tdp"].apply(normalize_wattage)
    df["socket"] = df["socket"].apply(normalize_socket)

    # Ensure numeric types
    df["cores"] = pd.to_numeric(df["cores"], errors="coerce").astype("Int64")
    df["threads"] = pd.to_numeric(df["threads"], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Drop rows with no name or price
    df = df.dropna(subset=["name", "price"])

    # Deduplicate (keep cheapest per component)
    df = deduplicate_by_price(df, "name")

    # Sort by manufacturer then price
    df = df.sort_values(["manufacturer", "price"]).reset_index(drop=True)

    # Export
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info(
        f"Processors cleaned: {original_count} raw -> {len(df)} clean -> {output_path}"
    )
    return df


def clean_gpus(input_path: str, output_path: str) -> pd.DataFrame:
    """Clean and normalize GPU data."""
    logger.info(f"Cleaning GPUs: {input_path}")
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    original_count = len(df)

    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

    df["manufacturer"] = df["manufacturer"].apply(normalize_manufacturer)
    df["base_clock"] = df["base_clock"].apply(normalize_clock_speed)
    df["boost_clock"] = df["boost_clock"].apply(normalize_clock_speed)
    df["memory"] = df["memory"].apply(normalize_memory)
    df["tdp"] = df["tdp"].apply(normalize_wattage)

    df["core_count"] = pd.to_numeric(df["core_count"], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df = df.dropna(subset=["name", "price"])
    df = deduplicate_by_price(df, "name")
    df = df.sort_values(["manufacturer", "price"]).reset_index(drop=True)

    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info(f"GPUs cleaned: {original_count} raw -> {len(df)} clean -> {output_path}")
    return df


def clean_motherboards(input_path: str, output_path: str) -> pd.DataFrame:
    """Clean and normalize motherboard data."""
    logger.info(f"Cleaning motherboards: {input_path}")
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    original_count = len(df)

    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

    df["manufacturer"] = df["manufacturer"].apply(normalize_manufacturer)
    df["socket"] = df["socket"].apply(normalize_socket)
    df["form_factor"] = df["form_factor"].apply(normalize_form_factor)
    df["max_memory"] = df["max_memory"].apply(normalize_memory)

    # Numeric columns
    for col in ["memory_slots", "pcie_slots", "sata_ports", "m2_slots"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df = df.dropna(subset=["name", "price"])
    df = deduplicate_by_price(df, "name")
    df = df.sort_values(["manufacturer", "price"]).reset_index(drop=True)

    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info(
        f"Motherboards cleaned: {original_count} raw -> {len(df)} clean -> {output_path}"
    )
    return df


def clean_ram(input_path: str, output_path: str) -> pd.DataFrame:
    """Clean and normalize RAM data."""
    logger.info(f"Cleaning RAM: {input_path}")
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    original_count = len(df)

    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

    df["manufacturer"] = df["manufacturer"].apply(normalize_manufacturer)
    df["speed"] = df["speed"].apply(normalize_clock_speed)
    df["voltage"] = df["voltage"].apply(normalize_voltage)
    df["ram_type"] = df["ram_type"].apply(normalize_ram_type)

    df["cas_latency"] = pd.to_numeric(df["cas_latency"], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Normalize boolean RGB field
    df["rgb"] = df["rgb"].apply(
        lambda x: True if str(x).strip().lower() in ("true", "1", "yes") else False
    )

    df = df.dropna(subset=["name", "price"])
    df = deduplicate_by_price(df, "name")
    df = df.sort_values(["manufacturer", "price"]).reset_index(drop=True)

    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info(f"RAM cleaned: {original_count} raw -> {len(df)} clean -> {output_path}")
    return df


# ══════════════════════════════════════════════════════════════════
#  CLEANING STATISTICS & REPORTING
# ══════════════════════════════════════════════════════════════════

def generate_cleaning_stats(
    raw_path: str, cleaned_path: str, component: str
) -> dict:
    """Compare raw vs cleaned data and generate quality metrics."""
    raw_df = pd.read_csv(raw_path)
    clean_df = pd.read_csv(cleaned_path)

    stats = {
        "component": component,
        "timestamp": datetime.now().isoformat(),
        "raw_rows": len(raw_df),
        "cleaned_rows": len(clean_df),
        "duplicates_removed": len(raw_df) - len(clean_df),
        "duplicate_pct": round(
            (len(raw_df) - len(clean_df)) / len(raw_df) * 100, 1
        ) if len(raw_df) > 0 else 0,
        "null_values_in_raw": int(raw_df.isnull().sum().sum()),
        "null_values_in_clean": int(clean_df.isnull().sum().sum()),
        "price_range": {
            "min": float(clean_df["price"].min()) if "price" in clean_df else None,
            "max": float(clean_df["price"].max()) if "price" in clean_df else None,
            "mean": round(float(clean_df["price"].mean()), 2)
            if "price" in clean_df else None,
        },
        "unique_manufacturers": int(clean_df["manufacturer"].nunique())
        if "manufacturer" in clean_df else 0,
        "sources": list(raw_df["source"].unique()) if "source" in raw_df else [],
    }
    return stats


# ══════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════

CLEANERS = {
    "processors": (
        os.path.join(RAW_DIR, "processors_raw.csv"),
        os.path.join(CLEANED_DIR, "processors_cleaned.csv"),
        clean_processors,
    ),
    "gpus": (
        os.path.join(RAW_DIR, "gpus_raw.csv"),
        os.path.join(CLEANED_DIR, "gpus_cleaned.csv"),
        clean_gpus,
    ),
    "motherboards": (
        os.path.join(RAW_DIR, "motherboards_raw.csv"),
        os.path.join(CLEANED_DIR, "motherboards_cleaned.csv"),
        clean_motherboards,
    ),
    "ram": (
        os.path.join(RAW_DIR, "ram_raw.csv"),
        os.path.join(CLEANED_DIR, "ram_cleaned.csv"),
        clean_ram,
    ),
}


def main():
    parser = argparse.ArgumentParser(
        description="ForgeSavant Data Cleaner & Normalizer"
    )
    parser.add_argument(
        "--component",
        choices=list(CLEANERS.keys()),
        help="Component type to clean",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Clean all component types",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print cleaning statistics after processing",
    )

    args = parser.parse_args()
    os.makedirs(CLEANED_DIR, exist_ok=True)

    components_to_clean = []
    if args.all:
        components_to_clean = list(CLEANERS.keys())
    elif args.component:
        components_to_clean = [args.component]
    else:
        parser.print_help()
        return

    all_stats = []

    for component in components_to_clean:
        raw_path, clean_path, cleaner_fn = CLEANERS[component]
        if not os.path.exists(raw_path):
            logger.warning(f"Raw data not found: {raw_path}, skipping {component}")
            continue

        cleaner_fn(raw_path, clean_path)

        if args.stats:
            stats = generate_cleaning_stats(raw_path, clean_path, component)
            all_stats.append(stats)

    if args.stats and all_stats:
        print(f"\n{'='*60}")
        print("DATA CLEANING REPORT")
        print(f"{'='*60}")
        for s in all_stats:
            print(f"\n  [{s['component'].upper()}]")
            print(f"    Raw rows:      {s['raw_rows']}")
            print(f"    Cleaned rows:  {s['cleaned_rows']}")
            print(f"    Duplicates:    {s['duplicates_removed']} ({s['duplicate_pct']}%)")
            print(f"    Manufacturers: {s['unique_manufacturers']}")
            print(f"    Price range:   ₹{s['price_range']['min']:,.0f} - ₹{s['price_range']['max']:,.0f}")
            print(f"    Avg price:     ₹{s['price_range']['mean']:,.0f}")
            print(f"    Sources:       {', '.join(s['sources'])}")
        print(f"\n{'='*60}\n")

        # Save stats to JSON
        stats_path = os.path.join(CLEANED_DIR, "cleaning_report.json")
        with open(stats_path, "w") as f:
            json.dump(all_stats, f, indent=2)
        logger.info(f"Stats saved to {stats_path}")


if __name__ == "__main__":
    main()
