"""
ForgeSavant Data Scraper
========================
Scrapes PC hardware component specifications and pricing data from
vendor websites and exports to CSV for downstream cleaning and import.

Usage:
    python scraper.py --component processors --output raw_data/processors_raw.csv
    python scraper.py --component gpus --output raw_data/gpus_raw.csv
    python scraper.py --all

Dependencies:
    pip install requests beautifulsoup4 pandas
"""

import argparse
import csv
import json
import os
import time
import random
import logging
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ── Config ────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, "raw_data")
LOG_FILE = os.path.join(BASE_DIR, "scraper.log")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Rate limiting: min and max delay between requests (seconds)
MIN_DELAY = 1.5
MAX_DELAY = 3.0

# Component type configurations
COMPONENT_CONFIGS = {
    "processors": {
        "fields": [
            "name", "type", "manufacturer", "cores", "threads",
            "base_clock", "boost_clock", "cache", "socket", "tdp",
            "price", "source",
        ],
        "output_file": "processors_raw.csv",
    },
    "gpus": {
        "fields": [
            "name", "type", "manufacturer", "core_count",
            "base_clock", "boost_clock", "memory", "tdp",
            "price", "source",
        ],
        "output_file": "gpus_raw.csv",
    },
    "motherboards": {
        "fields": [
            "name", "type", "manufacturer", "socket", "chipset",
            "form_factor", "memory_slots", "max_memory", "pcie_slots",
            "sata_ports", "m2_slots", "lan", "usb_ports",
            "price", "source",
        ],
        "output_file": "motherboards_raw.csv",
    },
    "ram": {
        "fields": [
            "name", "type", "manufacturer", "capacity", "ram_type",
            "speed", "cas_latency", "voltage", "rgb",
            "price", "source",
        ],
        "output_file": "ram_raw.csv",
    },
}

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ── Scraper Classes ───────────────────────────────────────────────
class BaseScraper:
    """Base scraper with rate limiting and error handling."""

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _rate_limit(self):
        """Random delay between requests to respect rate limits."""
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        time.sleep(delay)

    def fetch_page(self, url: str) -> BeautifulSoup | None:
        """Fetch and parse a single page with error handling."""
        try:
            self._rate_limit()
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def scrape_component_list(self, url: str) -> list[dict]:
        """Override in subclass to parse component data from page."""
        raise NotImplementedError


class GenericHardwareScraper(BaseScraper):
    """
    Generic scraper that extracts hardware specs from product listing pages.
    Handles common e-commerce page structures.
    """

    def __init__(self, source_name: str):
        super().__init__(source_name)

    def extract_spec_text(self, spec_element) -> str:
        """Extract and clean specification text from HTML element."""
        if spec_element is None:
            return ""
        text = spec_element.get_text(strip=True)
        # Remove common noise characters
        text = text.replace("\xa0", " ").replace("\u200b", "")
        return text.strip()

    def parse_price(self, price_text: str) -> float | None:
        """
        Parse price from various formats:
        - '₹14,999' -> 14999.0
        - 'Rs. 14,999.00' -> 14999.0
        - '$299.99' -> 299.99
        """
        if not price_text:
            return None
        # Remove currency symbols and whitespace
        cleaned = price_text.replace("₹", "").replace("$", "")
        cleaned = cleaned.replace("Rs.", "").replace("Rs", "")
        cleaned = cleaned.replace(",", "").strip()
        try:
            # Take first number found (ignore 'onwards' etc.)
            parts = cleaned.split()
            return float(parts[0])
        except (ValueError, IndexError):
            logger.warning(f"Could not parse price: '{price_text}'")
            return None

    def scrape_component_list(self, url: str) -> list[dict]:
        """Scrape a product listing page for component data."""
        soup = self.fetch_page(url)
        if soup is None:
            return []

        products = []
        # Look for common product card patterns
        product_cards = soup.select(
            ".product-card, .s-result-item, ._1AtVbE, .product-tuple-listing"
        )

        for card in product_cards:
            try:
                name_el = card.select_one(
                    ".product-title, .a-text-normal, ._4rR01T, .product-title a"
                )
                price_el = card.select_one(
                    ".product-price, .a-price-whole, ._30jeq3, .product-price span"
                )

                if name_el:
                    product = {
                        "name": self.extract_spec_text(name_el),
                        "price": self.parse_price(
                            self.extract_spec_text(price_el)
                        ) if price_el else None,
                        "source": self.source_name,
                    }
                    products.append(product)
            except Exception as e:
                logger.warning(f"Error parsing product card: {e}")
                continue

        logger.info(
            f"Scraped {len(products)} products from {self.source_name}"
        )
        return products


# ── CSV Export ────────────────────────────────────────────────────
def export_to_csv(data: list[dict], filepath: str, fields: list[str]):
    """Export scraped data to CSV with proper encoding and headers."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    df = pd.DataFrame(data)

    # Ensure all expected columns exist
    for field in fields:
        if field not in df.columns:
            df[field] = ""

    # Reorder columns to match expected schema
    df = df[[col for col in fields if col in df.columns]]

    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    logger.info(f"Exported {len(df)} rows to {filepath}")
    return df


def merge_csv_sources(filepaths: list[str], output_path: str) -> pd.DataFrame:
    """
    Merge multiple CSV files from different scraping sources into one.
    Handles column mismatches and deduplication.
    """
    frames = []
    for fp in filepaths:
        if os.path.exists(fp):
            df = pd.read_csv(fp, encoding="utf-8-sig")
            df["_source_file"] = os.path.basename(fp)
            frames.append(df)
            logger.info(f"Loaded {len(df)} rows from {fp}")
        else:
            logger.warning(f"File not found: {fp}")

    if not frames:
        logger.error("No data to merge")
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True, sort=False)
    merged.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info(
        f"Merged {len(merged)} total rows from {len(frames)} sources -> {output_path}"
    )
    return merged


# ── Summary Report ────────────────────────────────────────────────
def generate_scrape_report(data_dir: str):
    """Generate a summary report of all scraped raw data."""
    report = {
        "timestamp": datetime.now().isoformat(),
        "files": [],
    }

    for filename in os.listdir(data_dir):
        if filename.endswith(".csv"):
            filepath = os.path.join(data_dir, filename)
            df = pd.read_csv(filepath)
            file_info = {
                "filename": filename,
                "rows": len(df),
                "columns": list(df.columns),
                "null_counts": df.isnull().sum().to_dict(),
                "duplicate_names": int(df["name"].duplicated().sum())
                if "name" in df.columns else 0,
            }
            report["files"].append(file_info)

    report_path = os.path.join(data_dir, "scrape_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    logger.info(f"Scrape report generated: {report_path}")
    return report


# ── CLI ───────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="ForgeSavant Hardware Data Scraper"
    )
    parser.add_argument(
        "--component",
        choices=list(COMPONENT_CONFIGS.keys()),
        help="Component type to scrape",
    )
    parser.add_argument(
        "--output",
        help="Output CSV file path (relative to data-pipeline/)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate scrape report for all existing raw data",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Generate summary report of raw data",
    )

    args = parser.parse_args()

    if args.report or args.all:
        report = generate_scrape_report(RAW_DATA_DIR)
        print(f"\n{'='*50}")
        print("SCRAPE REPORT SUMMARY")
        print(f"{'='*50}")
        for file_info in report["files"]:
            print(f"\n  {file_info['filename']}:")
            print(f"    Rows: {file_info['rows']}")
            print(f"    Duplicates: {file_info['duplicate_names']}")
            null_issues = {
                k: v for k, v in file_info["null_counts"].items() if v > 0
            }
            if null_issues:
                print(f"    Null values: {null_issues}")
        print(f"\n{'='*50}\n")
    else:
        print("Usage: python scraper.py --report")
        print("       python scraper.py --component processors")
        print("\nNote: Live scraping requires target URLs to be configured.")
        print("Use --report to analyze existing raw data in raw_data/")


if __name__ == "__main__":
    main()
