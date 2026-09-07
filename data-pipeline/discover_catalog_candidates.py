"""Propose new catalog products from a retailer's published sitemap.

The catalog is small enough that whole categories dead-end a build: two cabinets
and four storage devices cannot support a nine-step planner. This proposes
candidates so a reviewer can extend it without typing every field by hand.

What it will and will not infer
-------------------------------
A compatibility field is only taken when the product's own published name states
it unambiguously: a kit called "DDR5-6000" is DDR5, a supply called "750 Watt"
is 750W. That is reading a label, not interpreting prose. Every rule must match
exactly one distinct value or the candidate is rejected, so "ATX / Micro-ATX"
support lists and "DDR4 or DDR5" comparison pages drop out rather than guess.

It deliberately does not derive processor socket or graphics TDP, because
neither appears in a product name and both would have to be inferred from a
model number. Those categories still need a human.

Nothing here writes to MongoDB. The output is a proposal for review, and the
compatibility values it suggests are the ones a reviewer should check first.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from connectors.retailer_scraper import (
    ADAPTERS,
    RetailerPageUnavailable,
    RobotsDisallowed,
    ScraperSession,
    extract_product_offer,
    normalize_identifier,
    slug_identifiers,
    utc_now,
)


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_PATH = BASE_DIR / "catalog_candidates.json"

# category -> (field, pattern over the published name, slug filter)
# Each pattern must resolve to exactly one distinct value.
DERIVATION_RULES = {
    "ram": (
        "ram_type",
        r"\b(DDR[45])\b",
        r"(?:^|-)(?:ram|memory)(?:-|$)|ddr[45]",
    ),
    "storage": (
        "interface",
        r"\b(NVMe|SATA)\b",
        r"ssd|hdd|hard-drive|nvme",
    ),
    "power_supplies": (
        "wattage",
        r"\b(\d{3,4})\s*(?:W|Watt|Watts)\b",
        r"smps|power-supply|psu",
    ),
    "cabinets": (
        "motherboard_support",
        r"\b(E-ATX|Micro-ATX|Mini-ITX|M-ATX|mATX|ITX|ATX)\b",
        r"cabinet|tower|pc-case",
    ),
    "motherboards": (
        "chipset",
        r"\b([BHXZA]\d{3}[A-Z]?|Z\d{2,3})\b",
        r"motherboard",
    ),
}

# A chipset states its socket and memory generation. The compatibility engine
# already relies on this relationship, so recording it here is consistent rather
# than a new inference.
CHIPSET_PLATFORM = {
    "A520": ("AM4", "DDR4"), "B450": ("AM4", "DDR4"), "B550": ("AM4", "DDR4"),
    "X470": ("AM4", "DDR4"), "X570": ("AM4", "DDR4"),
    "A620": ("AM5", "DDR5"), "B650": ("AM5", "DDR5"), "B650E": ("AM5", "DDR5"),
    "X670": ("AM5", "DDR5"), "X670E": ("AM5", "DDR5"), "B850": ("AM5", "DDR5"),
    "X870": ("AM5", "DDR5"), "X870E": ("AM5", "DDR5"),
    "B660": ("LGA 1700", None), "B760": ("LGA 1700", None),
    "H610": ("LGA 1700", None), "H670": ("LGA 1700", None), "H770": ("LGA 1700", None),
    "Z690": ("LGA 1700", None), "Z790": ("LGA 1700", None),
    "B860": ("LGA 1851", "DDR5"), "Z890": ("LGA 1851", "DDR5"),
}

FORM_FACTOR_BY_CHIPSET_SUFFIX = {"M": "Micro-ATX", "I": "Mini-ITX"}

# Rough INR bounds for a single component of each kind. A price far outside its
# category usually means the listing is a bundle or a prebuilt system rather
# than the part it appears to be: a "Mini ITX Case with SMPS, CPU Cooler" was
# offered at 360,000. Outliers are flagged for a reviewer, not dropped, because
# genuinely expensive parts exist.
PLAUSIBLE_PRICE_INR = {
    "ram": (800, 200_000),
    "storage": (1_000, 90_000),
    "power_supplies": (1_000, 80_000),
    "cabinets": (1_000, 80_000),
    "motherboards": (3_000, 150_000),
}


def canonical_support(value: str) -> str:
    key = value.replace("-", "").replace(" ", "").casefold()
    return {
        "eatx": "E-ATX", "microatx": "Micro-ATX", "matx": "Micro-ATX",
        "miniitx": "Mini-ITX", "itx": "Mini-ITX", "atx": "ATX",
    }.get(key, value.upper())


def derive_field(category: str, name: str) -> tuple[str, str] | None:
    """Return (field, value) only when the name states exactly one value."""
    field, pattern, _ = DERIVATION_RULES[category]
    found = re.findall(pattern, name or "", re.I)
    if not found:
        return None
    values = {str(v).upper() for v in found}
    if category == "cabinets":
        values = {canonical_support(v) for v in found}
        # "Supports ATX / Micro-ATX / Mini-ITX" states a range, not one value.
        if len(values) != 1:
            return None
        return field, next(iter(values))
    if len(values) != 1:
        return None
    value = next(iter(values))
    if category == "power_supplies":
        return field, f"{int(value)}W"
    if category == "ram":
        return field, value
    if category == "storage":
        return field, "NVMe" if value == "NVME" else value
    return field, value


def platform_from_chipset(chipset: str) -> dict:
    """Socket, memory generation and board size implied by a chipset name."""
    base = re.sub(r"[MI]$", "", chipset.upper())
    socket, memory = CHIPSET_PLATFORM.get(base, (None, None))
    details = {"chipset": chipset.upper()}
    if socket:
        details["socket"] = socket
    if memory:
        details["memory_type"] = memory
    suffix = chipset.upper()[len(base):]
    if suffix in FORM_FACTOR_BY_CHIPSET_SUFFIX:
        details["form_factor"] = FORM_FACTOR_BY_CHIPSET_SUFFIX[suffix]
    return details


# Words that appear in product slugs and must never be mistaken for a part
# number. A fabricated identifier is worse than no candidate: it becomes the
# key that prices and deduplication match on.
SLUG_PROSE = {
    "corsair", "cooler", "master", "asus", "msi", "gigabyte", "asrock", "nzxt", "antec",
    "seagate", "samsung", "crucial", "kingston", "western", "digital", "gskill", "skill",
    "intel", "amd", "ryzen", "core", "radeon", "geforce", "nvidia", "teamgroup", "adata",
    "motherboard", "processor", "desktop", "graphics", "card", "cabinet", "case", "tower",
    "mid", "full", "mini", "micro", "gaming", "black", "white", "rgb", "wifi", "tempered",
    "glass", "airflow", "series", "edition", "gold", "plus", "bronze", "platinum", "modular",
    "smps", "psu", "power", "supply", "ssd", "hdd", "nvme", "sata", "memory", "ram", "kit",
    "internal", "solid", "state", "drive", "hard", "with", "and", "the", "for",
}


def looks_like_part_number(value: str) -> bool:
    """A part number mixes letters and digits and is not a run of English words."""
    compact = re.sub(r"[^A-Za-z0-9]", "", value)
    if len(compact) < 6 or not re.search(r"\d", compact) or not re.search(r"[A-Za-z]", compact):
        return False
    words = [w.casefold() for w in re.split(r"[^A-Za-z0-9]+", value) if w]
    if any(w in SLUG_PROSE for w in words):
        return False
    # Reject a bare capacity or speed such as 32gb, 3200mhz, 750w.
    return not re.fullmatch(r"(ddr[45])?\d+(gb|tb|mhz|w|watt)?", compact, re.I)


def candidate_identity(product: dict) -> dict:
    """Identifiers the page actually states. Nothing is derived from the slug.

    Slug-derived part numbers were tried and rejected. A slug reads
    seagate-barracuda-2tb-hard-drive-st2000dm008, and every rule that recovered
    ST2000DM008 from one product produced BARRACUDA-2TB or
    5600X-100-100000065BOX from the next. The part number is the key prices and
    deduplication match on, so a plausible-looking wrong one is worse than none:
    it silently binds a price to the wrong product.

    A candidate without a stated identifier is still worth proposing. It carries
    a name, price, specifications and a URL; only the identifier needs a person.
    """
    identity = {"manufacturerPartNumber": "", "gtin": "", "identitySource": "none"}

    stated = str(product.get("mpn") or "").strip()
    if stated and " " not in stated and looks_like_part_number(stated):
        identity.update(manufacturerPartNumber=stated.upper(), identitySource="page mpn")
        return identity

    gtin = str(product.get("gtin") or "").strip()
    if gtin.isdigit() and len(gtin) in (8, 12, 13, 14):
        identity.update(gtin=gtin, identitySource="page gtin")
        return identity

    sku = str(product.get("sku") or "").strip()
    if sku and " " not in sku and looks_like_part_number(sku):
        identity.update(manufacturerPartNumber=sku.upper(), identitySource="page sku")
    return identity


def discover(
    source: str,
    session: ScraperSession,
    categories: dict[str, int],
    *,
    max_fetches_per_category: int = 90,
) -> dict:
    adapter = ADAPTERS[source](session)
    urls = adapter.product_urls()
    results: dict[str, list[dict]] = {}
    rejected: dict[str, dict[str, int]] = {}

    for category, target in categories.items():
        field, _, slug_filter = DERIVATION_RULES[category]
        pool = [u for u in urls if re.search(slug_filter, u.rstrip("/").rsplit("/", 1)[-1], re.I)]
        # A sitemap is grouped by brand and product line, so the first N entries
        # are near-identical. Striding across the pool samples the range instead:
        # taking storage sequentially returned thirty NVMe drives and no SATA.
        if len(pool) > max_fetches_per_category:
            stride = len(pool) // max_fetches_per_category
            pool = pool[::stride]
        accepted, reasons, fetched = [], {}, 0

        for url in pool:
            if len(accepted) >= target or fetched >= max_fetches_per_category:
                break
            fetched += 1
            try:
                product = extract_product_offer(session.get(url))
            except (RobotsDisallowed, RetailerPageUnavailable):
                reasons["page unavailable"] = reasons.get("page unavailable", 0) + 1
                continue
            if not product:
                reasons["no product markup"] = reasons.get("no product markup", 0) + 1
                continue

            derived = derive_field(category, product["name"])
            if derived is None:
                reasons["name does not state exactly one value"] = reasons.get("name does not state exactly one value", 0) + 1
                continue
            identity = candidate_identity(product)
            low, high = PLAUSIBLE_PRICE_INR[category]
            price_outlier = not (low <= float(product["price"]) <= high)
            specifications = {derived[0]: derived[1]}
            if category == "motherboards":
                specifications = platform_from_chipset(derived[1])

            accepted.append({
                "category": category,
                "name": product["name"],
                "manufacturer": product.get("brand", ""),
                **identity,
                "price": product["price"],
                "currency": product["currency"],
                "availability": product["availability"],
                "imageUrl": product["image_url"],
                "retailerUrl": url,
                "retailerSku": product["sku"],
                "gtin": product["gtin"],
                # Read from the published name. A reviewer should confirm these
                # before the product influences a compatibility decision.
                "specifications": specifications,
                "derivedFrom": product["name"],
                # Almost certainly a bundle or prebuilt rather than the bare part.
                "priceOutlier": price_outlier,
            })

        results[category] = accepted
        rejected[category] = {"fetched": fetched, "poolSize": len(pool), **reasons}

    return {
        "schemaVersion": "1.0",
        "source": source,
        "discoveredAt": utc_now(),
        "counts": {c: len(v) for c, v in results.items()},
        "priceOutliers": {c: sum(1 for r in v if r["priceOutlier"]) for c, v in results.items()},
        "rejections": rejected,
        "candidates": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Propose catalog products from a retailer sitemap")
    parser.add_argument("--source", default="mdcomputers_in", choices=sorted(ADAPTERS))
    parser.add_argument("--component", action="append", default=[], choices=sorted(DERIVATION_RULES))
    parser.add_argument("--target", type=int, default=30, help="Accepted candidates to collect per category")
    parser.add_argument("--max-fetches", type=int, default=90, help="Request ceiling per category")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--min-delay", type=float, default=1.2)
    parser.add_argument("--max-delay", type=float, default=2.2)
    args = parser.parse_args()

    chosen = args.component or list(DERIVATION_RULES)
    session = ScraperSession(min_delay=args.min_delay, max_delay=args.max_delay)
    report = discover(
        args.source, session, {c: args.target for c in chosen},
        max_fetches_per_category=args.max_fetches,
    )
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "output": str(args.output),
        "counts": report["counts"],
        "priceOutliers": report["priceOutliers"],
        "rejections": report["rejections"],
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
