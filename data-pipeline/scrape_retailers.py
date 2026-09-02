"""Collect live retailer prices for the verified catalog and export them for review.

Discovery and matching are exact by construction: a page is only visited when a
verified manufacturer part number or an operator-supplied retailer identifier
resolves to exactly one product URL, and a price is only exported with the
manufacturer part number attached so the administrator import matches on it
rather than on a title.

This tool never writes to MongoDB and never edits catalog specifications. Its
output is an offer feed for the signed administrator preview/apply workflow,
which remains the only path that can mark a price live.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from connectors.retailer_scraper import (
    ADAPTERS,
    CatalogTarget,
    RetailerPageUnavailable,
    RobotsDisallowed,
    ScraperSession,
    utc_now,
)


BASE_DIR = Path(__file__).resolve().parent
IDENTITY_FILES = {
    "processors": "processors.json",
    "gpus": "gpus.json",
    "motherboards": "motherboards.json",
    "ram": "ram.json",
    "storage": "storage.json",
    "power_supplies": "power-supplies.json",
    "cabinets": "cabinets.json",
}
FEED_PATH = BASE_DIR / "authorized_offer_feed.json"
REPORT_PATH = BASE_DIR / "retailer_scrape_report.json"


def load_targets(identity_dir: Path, identifiers: dict[str, list[str]] | None = None) -> list[CatalogTarget]:
    identifiers = identifiers or {}
    targets = []
    for category, filename in IDENTITY_FILES.items():
        for row in json.loads((identity_dir / filename).read_text(encoding="utf-8-sig")):
            mpn = str(row.get("manufacturerPartNumber", "")).strip()
            if not mpn:
                continue
            targets.append(CatalogTarget(
                category=category,
                name=str(row.get("name", "")).strip(),
                manufacturer_part_number=mpn,
                identifiers=tuple(identifiers.get(mpn.upper(), ())),
            ))
    return targets


def load_identifier_map(path: Path | None) -> dict[str, list[str]]:
    """Read {manufacturerPartNumber: [retailer identifiers]} for sites needing them."""
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("records", payload) if isinstance(payload, dict) else payload
    mapping: dict[str, list[str]] = {}
    for row in rows:
        mpn = str(row.get("manufacturerPartNumber") or row.get("manufacturer_part_number") or "").strip().upper()
        value = str(row.get("asin") or row.get("identifier") or row.get("source_item_id") or "").strip()
        if mpn and value:
            mapping.setdefault(mpn, []).append(value)
    return mapping


def feed_row(target: CatalogTarget, offer) -> dict:
    return {
        "name": offer.name,
        "category": target.category,
        "source": offer.source,
        "source_item_id": offer.source_item_id,
        # Carried so the administrator import matches on an exact identity
        # rather than on a product title.
        "manufacturer_part_number": target.manufacturer_part_number,
        "price": offer.price,
        "currency": offer.currency,
        "availability": offer.availability,
        "source_url": offer.source_url,
        "image_url": offer.image_url,
        "observed_at": offer.collected_at,
    }


def scrape_source(source: str, targets: list[CatalogTarget], session: ScraperSession) -> dict:
    adapter = ADAPTERS[source](session)
    by_mpn = {target.manufacturer_part_number: target for target in targets}

    try:
        discovery = adapter.discover(targets)
    except (RobotsDisallowed, RetailerPageUnavailable) as error:
        return {
            "source": source,
            "status": "failed",
            "error": f"{type(error).__name__}: {error}",
            "offers": [],
            "ambiguous": {},
            "notListed": sorted(by_mpn),
            "failures": [],
        }

    offers = []
    failures = []
    collected_at = utc_now()
    for mpn, url in sorted(discovery.matches.items()):
        target = by_mpn[mpn]
        try:
            offer = adapter.fetch_offer(url, target, collected_at=collected_at)
            offers.append(feed_row(target, offer))
        except (RobotsDisallowed, RetailerPageUnavailable) as error:
            failures.append({"manufacturerPartNumber": mpn, "url": url, "error": f"{type(error).__name__}: {error}"})

    resolved = set(discovery.matches) | set(discovery.ambiguous)
    return {
        "source": source,
        "status": "succeeded",
        "offers": offers,
        "discovered": len(discovery.matches),
        # Listed under several URLs: actionable, needs a human decision.
        "ambiguous": discovery.ambiguous,
        # Not listed at this retailer at all: nothing to do.
        "notListed": sorted(set(by_mpn) - resolved),
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect live retailer prices for signed administrator review")
    parser.add_argument("--source", action="append", default=[], choices=sorted(ADAPTERS), help="Repeatable; defaults to mdcomputers_in")
    parser.add_argument("--component", action="append", default=[], choices=sorted(IDENTITY_FILES), help="Repeatable category filter")
    parser.add_argument("--limit", type=int, default=0, help="Only attempt the first N catalog products")
    parser.add_argument("--identifiers", type=Path, help="JSON map of part numbers to retailer identifiers (ASINs)")
    parser.add_argument("--output", type=Path, default=FEED_PATH)
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    parser.add_argument("--min-delay", type=float, default=1.5)
    parser.add_argument("--max-delay", type=float, default=3.0)
    parser.add_argument("--apply", action="store_true", help="Write the offer feed; omit to report without exporting")
    args = parser.parse_args()

    sources = args.source or ["mdcomputers_in"]
    targets = load_targets(BASE_DIR / "verified_identity", load_identifier_map(args.identifiers))
    if args.component:
        targets = [target for target in targets if target.category in set(args.component)]
    if args.limit > 0:
        targets = targets[: args.limit]

    session = ScraperSession(min_delay=args.min_delay, max_delay=args.max_delay)
    results = [scrape_source(source, targets, session) for source in sources]

    offers = [row for result in results for row in result["offers"]]
    report = {
        "schemaVersion": "1.0",
        "collectedAt": utc_now(),
        "catalogProducts": len(targets),
        "sources": [
            {key: value for key, value in result.items() if key != "offers"} | {"matched": len(result["offers"])}
            for result in results
        ],
        "totalOffers": len(offers),
        "exported": bool(args.apply),
    }
    args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    if args.apply and offers:
        args.output.write_text(json.dumps({
            "schemaVersion": "1.0",
            "collectedAt": report["collectedAt"],
            "offers": offers,
        }, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        report["output"] = str(args.output)
    elif args.apply:
        report["note"] = "No offers matched; nothing was written."
    else:
        report["note"] = "Report only. Re-run with --apply to export the feed for admin review."

    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if any(result["status"] == "succeeded" for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
