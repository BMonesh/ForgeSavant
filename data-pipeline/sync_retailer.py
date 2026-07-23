"""Export retailer search results into ForgeSavant's signed feed workflow.

The curated CSV remains the source of hardware specifications and sample prices.
This tool never mutates it. Conservatively matched retailer results can be
exported for review in the application's administrator import screen.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Iterable

import pandas as pd

from connectors import FlipkartAffiliateConnector, RetailOffer, RetailerAuthenticationError


BASE_DIR = Path(__file__).resolve().parent
CLEANED_DIR = BASE_DIR / "cleaned_data"
REPORT_PATH = BASE_DIR / "retailer_sync_report.json"
FEED_PATH = BASE_DIR / "authorized_offer_feed.json"
COMPONENT_FILES = {
    "processors": "processors_cleaned.csv",
    "gpus": "gpus_cleaned.csv",
    "motherboards": "motherboards_cleaned.csv",
    "ram": "ram_cleaned.csv",
    "storage": "storage_cleaned.csv",
    "power_supplies": "power_supplies_cleaned.csv",
    "cabinets": "cabinets_cleaned.csv",
}
NOISE_TOKENS = {"processor", "graphics", "card", "desktop", "gaming", "memory", "internal", "ssd", "hdd"}


def title_tokens(value: str) -> set[str]:
    return {
        token for token in re.findall(r"[a-z0-9]+", str(value).lower())
        if token not in NOISE_TOKENS and len(token) > 1
    }


def match_offer(catalog_name: str, offers: Iterable[RetailOffer]) -> RetailOffer | None:
    expected = title_tokens(catalog_name)
    if not expected:
        return None
    candidates = []
    for offer in offers:
        actual = title_tokens(offer.name)
        coverage = len(expected & actual) / len(expected)
        # Avoid replacing a component price based on a loosely related accessory.
        if coverage >= 0.8:
            candidates.append((coverage, -offer.price, offer))
    return max(candidates, default=(0, 0, None), key=lambda item: (item[0], item[1]))[2]


def feed_row(component: str, offer: RetailOffer) -> dict:
    return {
        "name": offer.name,
        "category": component,
        "source_item_id": offer.source_item_id,
        "price": offer.price,
        "currency": offer.currency,
        "availability": offer.availability,
        "source_url": offer.source_url,
        "image_url": offer.image_url,
        "observed_at": offer.collected_at,
    }


def load_fixture(path: Path) -> list[RetailOffer]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return FlipkartAffiliateConnector.parse_response(payload, collected_at=datetime.now(timezone.utc).isoformat())


def sync_component(component: str, connector) -> dict:
    path = CLEANED_DIR / COMPONENT_FILES[component]
    frame = pd.read_csv(path, encoding="utf-8-sig")
    matched = 0
    failures = []
    accepted_offers = []
    for _, row in frame.iterrows():
        try:
            offers = connector.search(str(row["name"]))
            offer = match_offer(str(row["name"]), offers)
            if offer:
                matched += 1
                accepted_offers.append(feed_row(component, offer))
            else:
                failures.append(str(row["name"]))
        except RetailerAuthenticationError:
            raise
        except Exception as error:  # keep one failed product from aborting the whole refresh
            failures.append(f"{row['name']}: {type(error).__name__}")
    return {"rows": len(frame), "matched": matched, "unmatched": failures, "offers": accepted_offers}


class FixtureConnector:
    def __init__(self, offers: list[RetailOffer]):
        self.offers = offers

    def search(self, _query: str) -> list[RetailOffer]:
        return self.offers


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync official retailer offers into curated catalog rows")
    parser.add_argument("--component", choices=[*COMPONENT_FILES, "all"], default="all")
    parser.add_argument("--apply", action="store_true", help="Export matched live offers for signed admin review; never changes catalog CSVs")
    parser.add_argument("--fixture", type=Path, help="Offline API response fixture (always marked fixture, never live)")
    args = parser.parse_args()

    if args.fixture:
        connector = FixtureConnector(load_fixture(args.fixture))
        if args.apply:
            parser.error("Fixture data cannot be exported for application; omit --apply")
    else:
        affiliate_id = os.getenv("FLIPKART_AFFILIATE_ID", "")
        affiliate_token = os.getenv("FLIPKART_AFFILIATE_TOKEN", "")
        if not affiliate_id or not affiliate_token:
            parser.error("Set FLIPKART_AFFILIATE_ID and FLIPKART_AFFILIATE_TOKEN, or use --fixture")
        connector = FlipkartAffiliateConnector(affiliate_id, affiliate_token)

    components = COMPONENT_FILES if args.component == "all" else [args.component]
    report = {
        "source": "flipkart_affiliate",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "mode": "fixture" if args.fixture else "live",
        "components": {component: sync_component(component, connector) for component in components},
    }
    if args.apply:
        offers = [offer for result in report["components"].values() for offer in result["offers"]]
        FEED_PATH.write_text(json.dumps({"source": "flipkart_affiliate", "offers": offers}, indent=2), encoding="utf-8")
        report["feed_path"] = str(FEED_PATH)
        report["exported"] = len(offers)
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
