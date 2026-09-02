"""Retailer page collection for live Indian price observations.

This connector reads public retailer product pages. That is a deliberate policy
choice by the repository owner, made after the affiliate and licensed-feed
routes were exhausted, and it is narrower than a general crawler:

- Only the exact catalog products are fetched. Discovery happens against a
  published sitemap or an operator-supplied identifier list, so a run costs one
  feed request plus at most one page per catalog product, not a site crawl.
- robots.txt is fetched and honoured per host, including Crawl-delay.
- A price is only attributed to a catalog product on an exact manufacturer part
  number or exact retailer identifier. Nothing is matched by title similarity,
  because a wrong attribution silently corrupts the price history.
- Output is a feed for the signed administrator review path. This module never
  writes to MongoDB and never edits catalog specifications.

Retailer terms of service are not the same thing as robots.txt. Honouring
robots.txt does not by itself make collection contractually permitted, and the
operator is responsible for that judgement per site.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import random
import re
import time
from typing import Callable, Iterable
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests

from .flipkart_affiliate import RetailOffer


DEFAULT_USER_AGENT = "ForgeSavantCatalog/1.0 (+https://github.com/BMonesh/ForgeSavant)"
AVAILABILITY_BY_SCHEMA = {
    "instock": "in_stock",
    "instoreonly": "in_stock",
    "limitedavailability": "in_stock",
    "onlineonly": "in_stock",
    "outofstock": "out_of_stock",
    "soldout": "out_of_stock",
    "discontinued": "out_of_stock",
    "backorder": "preorder",
    "preorder": "preorder",
    "presale": "preorder",
}


class RobotsDisallowed(RuntimeError):
    """Raised when robots.txt forbids the requested path."""


class RetailerPageUnavailable(RuntimeError):
    """Raised when a page cannot be retrieved or does not contain an offer."""


def normalize_identifier(value: object) -> str:
    """Collapse a part number or slug to comparable alphanumerics."""
    return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())


def slug_identifiers(slug: str, max_tokens: int = 6) -> set[str]:
    """Every contiguous run of slug tokens, normalized.

    Substring matching is unsafe here: BX8071513600K is a prefix of
    BX8071513600KF, which is a different processor, so `mpn in slug` would
    attach the KF price to the K. Part numbers do straddle separators though
    (AMD's 100-100000065BOX becomes two slug tokens), so an exact match against
    single tokens is too strict. Comparing against contiguous token runs is both
    exact at the boundaries and tolerant of internal separators.
    """
    tokens = [token for token in re.split(r"[^a-z0-9]+", str(slug or "").casefold()) if token]
    values = set()
    for start in range(len(tokens)):
        for end in range(start + 1, min(start + max_tokens, len(tokens)) + 1):
            values.add("".join(tokens[start:end]))
    return values


def parse_price(value: object) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    cleaned = re.sub(r"[^0-9.,]", "", text).replace(",", "")
    if cleaned.count(".") > 1:  # thousands separators written as dots
        head, _, tail = cleaned.rpartition(".")
        cleaned = head.replace(".", "") + "." + tail
    try:
        price = float(cleaned)
    except ValueError:
        return None
    return price if price > 0 else None


def normalize_availability(value: object) -> str:
    token = str(value or "").rsplit("/", 1)[-1].casefold().replace(" ", "")
    return AVAILABILITY_BY_SCHEMA.get(token, "unknown")


def iter_jsonld(html: str) -> Iterable[dict]:
    """Yield every JSON-LD object in the document, flattening @graph and lists."""
    for block in re.findall(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.S | re.I
    ):
        try:
            payload = json.loads(block.strip())
        except (json.JSONDecodeError, ValueError):
            continue
        pending = payload if isinstance(payload, list) else [payload]
        while pending:
            node = pending.pop(0)
            if not isinstance(node, dict):
                continue
            if isinstance(node.get("@graph"), list):
                pending.extend(node["@graph"])
                continue
            yield node


def extract_product_offer(html: str) -> dict | None:
    """Return {name, sku, mpn, gtin, price, currency, availability, image} or None.

    schema.org Product markup is preferred over CSS selectors: retailers publish
    it for machines, so it survives theme changes that break class names.
    """
    for node in iter_jsonld(html):
        types = node.get("@type")
        types = types if isinstance(types, list) else [types]
        if not any(str(value).casefold() in {"product", "productgroup"} for value in types):
            continue
        offers = node.get("offers") or {}
        if isinstance(offers, list):
            offers = next((item for item in offers if isinstance(item, dict)), {})
        if not isinstance(offers, dict):
            offers = {}
        price = parse_price(offers.get("price") or offers.get("lowPrice"))
        if price is None:
            continue
        image = node.get("image")
        if isinstance(image, list):
            image = next((item for item in image if item), "")
        if isinstance(image, dict):
            image = image.get("url", "")
        return {
            "name": str(node.get("name") or "").strip(),
            "sku": str(node.get("sku") or "").strip(),
            "mpn": str(node.get("mpn") or "").strip(),
            "gtin": str(node.get("gtin13") or node.get("gtin") or node.get("gtin12") or "").strip(),
            "price": price,
            "currency": str(offers.get("priceCurrency") or "INR").strip().upper(),
            "availability": normalize_availability(offers.get("availability")),
            "image_url": str(image or "").strip(),
        }
    return None


class ScraperSession:
    """Rate-limited, robots-aware fetcher."""

    def __init__(
        self,
        *,
        user_agent: str = DEFAULT_USER_AGENT,
        min_delay: float = 1.5,
        max_delay: float = 3.0,
        timeout: float = 25.0,
        session: requests.Session | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        obey_robots: bool = True,
    ):
        self.user_agent = user_agent
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.timeout = timeout
        self.sleeper = sleeper
        self.obey_robots = obey_robots
        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-IN,en;q=0.9",
        })
        self._robots: dict[str, RobotFileParser | None] = {}
        self._last_request = 0.0

    def _robots_for(self, url: str) -> RobotFileParser | None:
        origin = "{0.scheme}://{0.netloc}".format(urlparse(url))
        if origin not in self._robots:
            parser = RobotFileParser()
            parser.set_url(urljoin(origin, "/robots.txt"))
            try:
                response = self.session.get(urljoin(origin, "/robots.txt"), timeout=self.timeout)
                if response.status_code >= 400:
                    # No usable robots.txt. Treat as "not disallowed" but keep
                    # the conservative delay, which is what a polite client does.
                    self._robots[origin] = None
                else:
                    parser.parse(response.text.splitlines())
                    self._robots[origin] = parser
            except requests.RequestException:
                self._robots[origin] = None
        return self._robots[origin]

    def can_fetch(self, url: str) -> bool:
        if not self.obey_robots:
            return True
        parser = self._robots_for(url)
        return True if parser is None else parser.can_fetch(self.user_agent, url)

    def crawl_delay(self, url: str) -> float | None:
        parser = self._robots_for(url)
        if parser is None:
            return None
        try:
            value = parser.crawl_delay(self.user_agent)
        except AttributeError:
            return None
        return float(value) if value else None

    def _wait(self, url: str) -> None:
        delay = max(
            random.uniform(self.min_delay, self.max_delay),
            self.crawl_delay(url) or 0.0,
        )
        elapsed = time.monotonic() - self._last_request if self._last_request else delay
        if elapsed < delay:
            self.sleeper(delay - elapsed)
        self._last_request = time.monotonic()

    def get(self, url: str) -> str:
        if not self.can_fetch(url):
            raise RobotsDisallowed(f"robots.txt disallows {url}")
        self._wait(url)
        try:
            response = self.session.get(url, timeout=self.timeout)
        except requests.RequestException as error:
            raise RetailerPageUnavailable(f"{type(error).__name__} fetching {url}") from error
        if response.status_code != 200:
            raise RetailerPageUnavailable(f"HTTP {response.status_code} fetching {url}")
        return response.text


@dataclass(frozen=True)
class CatalogTarget:
    """One verified catalog product the scraper is allowed to look for."""

    category: str
    name: str
    manufacturer_part_number: str
    identifiers: tuple[str, ...] = ()

    def keys(self) -> tuple[str, ...]:
        values = [self.manufacturer_part_number, *self.identifiers]
        return tuple(key for key in (normalize_identifier(value) for value in values) if key)


@dataclass(frozen=True)
class Discovery:
    """Split discovery outcomes so ambiguity is never mistaken for absence.

    A product the retailer stocks under several URLs needs a human decision; a
    product it does not stock needs nothing. Reporting both as "unmatched" would
    hide work that is actually actionable.
    """

    matches: dict[str, str]
    ambiguous: dict[str, list[str]]


class RetailerAdapter:
    """Base adapter. Subclasses map catalog targets to exactly one retailer page."""

    source = ""
    base_url = ""

    def __init__(self, session: ScraperSession):
        self.session = session

    def discover(self, targets: list[CatalogTarget]) -> Discovery:
        """Resolve catalog targets to retailer product URLs."""
        raise NotImplementedError

    def fetch_offer(self, url: str, target: CatalogTarget, *, collected_at: str) -> RetailOffer:
        html = self.session.get(url)
        product = extract_product_offer(html)
        if product is None:
            raise RetailerPageUnavailable(f"no schema.org Product offer found at {url}")
        source_item_id = product["sku"] or product["mpn"] or self.identifier_from_url(url)
        if not source_item_id:
            raise RetailerPageUnavailable(f"no retailer item id available at {url}")
        return RetailOffer(
            source_item_id=source_item_id,
            name=product["name"] or target.name,
            price=product["price"],
            currency=product["currency"] or "INR",
            availability=product["availability"],
            source=self.source,
            source_url=url,
            image_url=product["image_url"],
            collected_at=collected_at,
        )

    @staticmethod
    def identifier_from_url(url: str) -> str:
        return urlparse(url).path.rstrip("/").rsplit("/", 1)[-1][:64]


class MDComputersAdapter(RetailerAdapter):
    """mdcomputers.in — OpenCart with a published product sitemap.

    The product URL slug embeds the manufacturer part number, so discovery is a
    single sitemap fetch matched offline against the verified catalog. No search
    queries and no crawling are required.
    """

    source = "mdcomputers_in"
    base_url = "https://mdcomputers.in"
    sitemap_url = "https://mdcomputers.in/feed_products.xml"

    def product_urls(self) -> list[str]:
        document = self.session.get(self.sitemap_url)
        urls = re.findall(r"<loc>\s*<!\[CDATA\[(.*?)\]\]>\s*</loc>", document, re.S)
        if not urls:
            urls = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", document)
        return [url.strip() for url in urls if "/product/" in url]

    def discover(self, targets: list[CatalogTarget]) -> Discovery:
        slugs = [(url, slug_identifiers(self.identifier_from_url(url))) for url in self.product_urls()]
        matches: dict[str, str] = {}
        ambiguous: dict[str, list[str]] = {}
        for target in targets:
            keys = target.keys()
            if not keys:
                continue
            found = [url for url, candidates in slugs if candidates & set(keys)]
            if len(found) == 1:
                matches[target.manufacturer_part_number] = found[0]
            elif found:
                # The retailer lists this part number under several URLs, often a
                # bundle or a bare-tray variant. A human resolves that in the
                # admin screen; guessing would attach a price to the wrong SKU.
                ambiguous[target.manufacturer_part_number] = sorted(found)[:8]
        return Discovery(matches=matches, ambiguous=ambiguous)


class AmazonIndiaAdapter(RetailerAdapter):
    """amazon.in — reached only through operator-supplied ASINs.

    Amazon's Conditions of Use prohibit data gathering tools regardless of what
    robots.txt permits, so this adapter deliberately does no discovery: it will
    not search, browse, or crawl. It fetches a product page only when the
    operator has already established the catalog-to-ASIN relationship through
    the reviewed affiliate-link mapping.
    """

    source = "amazon_in"
    base_url = "https://www.amazon.in"

    ASIN_PATTERN = re.compile(r"^[A-Z0-9]{10}$")

    def discover(self, targets: list[CatalogTarget]) -> Discovery:
        matches: dict[str, str] = {}
        ambiguous: dict[str, list[str]] = {}
        for target in targets:
            asins = [
                value.upper() for value in target.identifiers
                if self.ASIN_PATTERN.match(str(value).upper())
            ]
            urls = [f"{self.base_url}/dp/{asin}" for asin in dict.fromkeys(asins)]
            if len(urls) == 1:
                matches[target.manufacturer_part_number] = urls[0]
            elif urls:
                ambiguous[target.manufacturer_part_number] = urls
        return Discovery(matches=matches, ambiguous=ambiguous)

    @staticmethod
    def identifier_from_url(url: str) -> str:
        match = re.search(r"/dp/([A-Za-z0-9]{10})", url)
        return match.group(1).upper() if match else ""

    def fetch_offer(self, url: str, target: CatalogTarget, *, collected_at: str) -> RetailOffer:
        try:
            return super().fetch_offer(url, target, collected_at=collected_at)
        except RetailerPageUnavailable:
            # Amazon serves a bot challenge instead of an error status, and the
            # challenge page carries no Product markup. Surfacing that honestly
            # is better than falling back to scraping the rendered DOM.
            raise RetailerPageUnavailable(
                f"no structured offer at {url}; the page was likely a bot challenge"
            )


ADAPTERS = {
    MDComputersAdapter.source: MDComputersAdapter,
    AmazonIndiaAdapter.source: AmazonIndiaAdapter,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
