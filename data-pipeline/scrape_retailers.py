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


def load_resolutions(path: Path | None) -> dict[str, set[str]]:
    """Read the product URLs an operator chose, keyed by manufacturer part number.

    A part number listed under several URLs is ambiguous by design and is never
    guessed. This is how a human records the decision once so later runs can act
    on it, mirroring how amazon.in takes operator-supplied ASINs. The chosen page
    still has to corroborate the part number before its price is used.

    A part number can be ambiguous at more than one retailer, so each maps to a
    set of URLs; a retailer resolves only when exactly one of its own candidates
    appears in that set, which keeps one retailer's choice from being applied to
    another's listing.
    """
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("records", payload) if isinstance(payload, dict) else payload
    resolutions: dict[str, set[str]] = {}
    for row in rows:
        mpn = str(row.get("manufacturerPartNumber") or row.get("manufacturer_part_number") or "").strip()
        url = str(row.get("url") or row.get("source_url") or "").strip()
        if mpn and url.startswith("https://"):
            resolutions.setdefault(mpn.upper(), set()).add(url)
    return resolutions


def inspect_candidates(
    adapter,
    ambiguous: dict[str, list[str]],
    by_mpn: dict[str, CatalogTarget] | None = None,
) -> dict[str, dict]:
    """Fetch each ambiguous candidate so an operator can choose from real data.

    Opt-in, because it costs one request per candidate. Reading the pages is the
    only way to tell an i7-13700K listing from the KF sitting beside it when both
    slugs carry the same part number.
    """
    from connectors.retailer_scraper import extract_product_offer

    by_mpn = by_mpn or {}
    inspected: dict[str, dict] = {}
    for mpn, urls in sorted(ambiguous.items()):
        rows = []
        for url in urls:
            entry = {"url": url}
            try:
                product = extract_product_offer(adapter.session.get(url))
                if product:
                    entry.update({
                        "name": product["name"],
                        "sku": product["sku"],
                        "price": product["price"],
                        "availability": product["availability"],
                    })
                else:
                    entry["error"] = "no schema.org Product offer"
            except (RobotsDisallowed, RetailerPageUnavailable) as error:
                entry["error"] = f"{type(error).__name__}: {error}"
            rows.append(entry)
        target = by_mpn.get(mpn)
        inspected[mpn] = {
            # The verified catalog name is what the operator matches against;
            # the variants differ by a WIFI or DDR4 suffix the slug does not carry.
            "catalogName": target.name if target else "",
            "candidates": rows,
        }
    return inspected


def feed_row(target: CatalogTarget, offer, *, permits_ai_training: bool = True) -> dict:
    return {
        "name": offer.name,
        "category": target.category,
        "source": offer.source,
        # Some retailers permit their content to be shown but not used as model
        # training data. Recording that alongside the price keeps the two
        # permissions from being conflated once the offer leaves this tool.
        "ai_training_permitted": bool(permits_ai_training),
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


def price_disagreements(offers: list[dict], tolerance: float = 0.25) -> list[dict]:
    """Products whose retailers disagree by more than `tolerance` of the lower price.

    Two independent sources quoting the same part number is the cheapest
    correctness check available. A wide spread usually means one side is a stale
    out-of-stock listing or a mis-identified page, and a reviewer should see that
    before either price is applied.
    """
    by_part: dict[str, list[dict]] = {}
    for offer in offers:
        by_part.setdefault(offer["manufacturer_part_number"], []).append(offer)

    flagged = []
    for part_number, rows in sorted(by_part.items()):
        prices = [row["price"] for row in rows if row.get("price")]
        if len(prices) < 2:
            continue
        low, high = min(prices), max(prices)
        if low > 0 and (high - low) / low > tolerance:
            flagged.append({
                "manufacturerPartNumber": part_number,
                "spread": round((high - low) / low, 3),
                "quotes": sorted(
                    ({"source": row["source"], "price": row["price"], "availability": row["availability"]}
                     for row in rows),
                    key=lambda quote: quote["price"],
                ),
            })
    return sorted(flagged, key=lambda row: -row["spread"])


def scrape_source(
    source: str,
    targets: list[CatalogTarget],
    session: ScraperSession,
    *,
    resolutions: dict[str, set[str]] | None = None,
    inspect_ambiguous: bool = False,
) -> dict:
    adapter = ADAPTERS[source](session)
    by_mpn = {target.manufacturer_part_number: target for target in targets}
    resolutions = resolutions or {}

    try:
        discovery = adapter.discover(targets)
    except (RobotsDisallowed, RetailerPageUnavailable) as error:
        return {
            "source": source,
            "status": "failed",
            "error": f"{type(error).__name__}: {error}",
            "offers": [],
            "resolvedByOperator": [],
            "ambiguous": {},
            "notListed": sorted(by_mpn),
            "failures": [],
        }

    # An operator's recorded choice settles an ambiguity; the chosen page must
    # still corroborate the part number in fetch_offer before it is used.
    matches = dict(discovery.matches)
    ambiguous = dict(discovery.ambiguous)
    resolved = []
    for mpn in list(ambiguous):
        chosen = resolutions.get(mpn.upper(), set()) & set(ambiguous[mpn])
        # Exactly one of this retailer's candidates must be chosen. Two would be
        # a contradictory instruction, and zero means the operator resolved this
        # part number at a different retailer.
        if len(chosen) == 1:
            matches[mpn] = chosen.pop()
            ambiguous.pop(mpn)
            resolved.append(mpn)

    offers = []
    failures = []
    collected_at = utc_now()
    for mpn, url in sorted(matches.items()):
        target = by_mpn[mpn]
        try:
            offer = adapter.fetch_offer(url, target, collected_at=collected_at)
            offers.append(feed_row(target, offer, permits_ai_training=adapter.permits_ai_training))
        except (RobotsDisallowed, RetailerPageUnavailable) as error:
            failures.append({"manufacturerPartNumber": mpn, "url": url, "error": f"{type(error).__name__}: {error}"})

    seen = set(matches) | set(ambiguous)
    result = {
        "source": source,
        "status": "succeeded",
        "offers": offers,
        "discovered": len(matches),
        "resolvedByOperator": sorted(resolved),
        # Listed under several URLs: actionable, needs a human decision.
        "ambiguous": ambiguous,
        # Not listed at this retailer at all: nothing to do.
        "notListed": sorted(set(by_mpn) - seen),
        "failures": failures,
    }
    if inspect_ambiguous and ambiguous:
        result["ambiguousCandidates"] = inspect_candidates(adapter, ambiguous, by_mpn)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect live retailer prices for signed administrator review")
    parser.add_argument("--source", action="append", default=[], choices=sorted(ADAPTERS), help="Repeatable; defaults to the two sitemap-discoverable retailers")
    parser.add_argument("--component", action="append", default=[], choices=sorted(IDENTITY_FILES), help="Repeatable category filter")
    parser.add_argument("--limit", type=int, default=0, help="Only attempt the first N catalog products")
    parser.add_argument("--identifiers", type=Path, help="JSON map of part numbers to retailer identifiers (ASINs)")
    parser.add_argument("--resolutions", type=Path, help="JSON map of part numbers to an operator-chosen product URL")
    parser.add_argument("--inspect-ambiguous", action="store_true", help="Fetch each ambiguous candidate so the report shows what to choose between")
    parser.add_argument("--output", type=Path, default=FEED_PATH)
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    parser.add_argument("--min-delay", type=float, default=1.5)
    parser.add_argument("--max-delay", type=float, default=3.0)
    parser.add_argument("--apply", action="store_true", help="Write the offer feed; omit to report without exporting")
    args = parser.parse_args()

    sources = args.source or ["mdcomputers_in", "primeabgb_com"]
    targets = load_targets(BASE_DIR / "verified_identity", load_identifier_map(args.identifiers))
    if args.component:
        targets = [target for target in targets if target.category in set(args.component)]
    if args.limit > 0:
        targets = targets[: args.limit]

    session = ScraperSession(min_delay=args.min_delay, max_delay=args.max_delay)
    resolutions = load_resolutions(args.resolutions)
    results = [
        scrape_source(source, targets, session, resolutions=resolutions, inspect_ambiguous=args.inspect_ambiguous)
        for source in sources
    ]

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
        "productsCovered": len({row["manufacturer_part_number"] for row in offers}),
        # Reviewer-facing: two sources disagreeing on the same part number means
        # at least one of them should not be applied.
        "priceDisagreements": price_disagreements(offers),
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
