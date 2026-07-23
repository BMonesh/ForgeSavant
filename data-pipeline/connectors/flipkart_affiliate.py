"""Flipkart Affiliate API connector.

This connector uses Flipkart's documented affiliate search API. It deliberately
does not scrape storefront HTML. Credentials are read by the CLI and passed in;
they are never persisted in output files.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import time
from typing import Callable

import requests


SEARCH_URL = "https://affiliate-api.flipkart.net/affiliate/1.0/search.json"


class RetailerAuthenticationError(RuntimeError):
    """Raised when retailer credentials are missing, invalid, or forbidden."""


@dataclass(frozen=True)
class RetailOffer:
    source_item_id: str
    name: str
    price: float
    currency: str
    availability: str
    source: str
    source_url: str
    image_url: str
    collected_at: str

    def to_dict(self) -> dict:
        return asdict(self)


def _amount(value) -> float | None:
    if isinstance(value, dict):
        value = value.get("amount")
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _best_image(image_urls) -> str:
    if not isinstance(image_urls, dict):
        return ""
    for key in ("800x800", "400x400", "200x200", "unknown"):
        if image_urls.get(key):
            return str(image_urls[key]).replace("http://", "https://", 1)
    return str(next(iter(image_urls.values()), "")).replace("http://", "https://", 1)


class FlipkartAffiliateConnector:
    """Small, retrying client for the official keyword-search endpoint."""

    def __init__(
        self,
        affiliate_id: str,
        affiliate_token: str,
        *,
        session: requests.Session | None = None,
        retries: int = 3,
        timeout: float = 15,
        sleeper: Callable[[float], None] = time.sleep,
        min_interval: float = 0.06,
    ):
        if not affiliate_id or not affiliate_token:
            raise ValueError("Flipkart affiliate id and token are required")
        self.session = session or requests.Session()
        self.session.headers.update({
            "Fk-Affiliate-Id": affiliate_id,
            "Fk-Affiliate-Token": affiliate_token,
            "Accept": "application/json",
            "User-Agent": "ForgeSavantCatalog/1.0",
        })
        self.retries = max(1, retries)
        self.timeout = timeout
        self.sleeper = sleeper
        self.min_interval = max(0, min_interval)
        self._last_request_at = 0.0

    def search(self, query: str, result_count: int = 10) -> list[RetailOffer]:
        if not query.strip():
            return []
        response = None
        for attempt in range(self.retries):
            try:
                wait_for = self.min_interval - (time.monotonic() - self._last_request_at)
                if wait_for > 0:
                    self.sleeper(wait_for)
                response = self.session.get(
                    SEARCH_URL,
                    params={"query": query, "resultCount": min(max(result_count, 1), 10)},
                    timeout=self.timeout,
                )
                self._last_request_at = time.monotonic()
                if response.status_code in (401, 403):
                    raise RetailerAuthenticationError("Flipkart rejected the affiliate credentials")
                response.raise_for_status()
                return self.parse_response(response.json())
            except RetailerAuthenticationError:
                raise
            except (requests.RequestException, ValueError):
                if attempt == self.retries - 1:
                    raise
                self.sleeper(2 ** attempt)
        return []

    @staticmethod
    def parse_response(payload: dict, collected_at: str | None = None) -> list[RetailOffer]:
        timestamp = collected_at or datetime.now(timezone.utc).isoformat()
        offers = []
        for entry in payload.get("productInfoList", []):
            base = entry.get("productBaseInfoV1") or entry.get("productBaseInfo") or {}
            shipping = entry.get("productShippingInfoV1") or entry.get("productShippingInfo") or {}
            price = _amount(base.get("flipkartSpecialPrice"))
            if price is None:
                price = _amount(base.get("flipkartSellingPrice"))
            if not base.get("title") or price is None:
                continue
            in_stock = shipping.get("inStock")
            availability = "in_stock" if in_stock is True else "out_of_stock" if in_stock is False else "unknown"
            offers.append(RetailOffer(
                source_item_id=str(base.get("productId", "")),
                name=str(base["title"]).strip(),
                price=price,
                currency=str((base.get("flipkartSellingPrice") or {}).get("currency", "INR")),
                availability=availability,
                source="flipkart_affiliate",
                source_url=str(base.get("productUrl", "")),
                image_url=_best_image(base.get("imageUrls")),
                collected_at=timestamp,
            ))
        return offers
