"""Authenticated Open Icecat product-content connector.

The connector performs bounded product lookups by manufacturer part number. It
does not download the full Icecat index and never places credentials in URLs or
serialized output.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import re
import time
from typing import Callable
from urllib.parse import urlencode

from defusedxml import ElementTree as ET
import requests


PRODUCT_URL = "https://data.icecat.biz/xml_s3/xml_server3.cgi"


class IcecatAuthenticationError(RuntimeError):
    """Raised when Icecat rejects the configured account credentials."""


@dataclass(frozen=True)
class CatalogObservation:
    schema_version: str
    source: str
    source_tier: str
    source_product_id: str
    manufacturer: str
    manufacturer_part_number: str
    source_reported_part_number: str
    name: str
    category: str
    gtins: list[str]
    image_url: str
    manufacturer_url: str
    source_record_url: str
    specifications: dict
    observed_at: str
    raw_sha256: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class IcecatLookupResult:
    status: str
    observation: CatalogObservation | None = None
    error: str = ""
    raw_xml: str = ""

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "observation": self.observation.to_dict() if self.observation else None,
            "error": self.error,
        }


def _feature_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _put_feature(target: dict, key: str, value: str) -> None:
    if not key or not value:
        return
    if key not in target:
        target[key] = value
    elif target[key] != value:
        previous = target[key] if isinstance(target[key], list) else [target[key]]
        if value not in previous:
            previous.append(value)
        target[key] = previous


class OpenIcecatConnector:
    """Small retrying client for the documented Icecat product XML endpoint."""

    def __init__(
        self,
        username: str,
        password: str,
        *,
        language: str = "EN",
        session: requests.Session | None = None,
        retries: int = 3,
        timeout: float = 20,
        sleeper: Callable[[float], None] = time.sleep,
        min_interval: float = 0.25,
    ):
        if not username or not password:
            raise ValueError("Icecat username and password are required")
        self.session = session or requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({
            "Accept": "application/xml",
            "User-Agent": "ForgeSavantCatalog/1.0",
        })
        self.language = language.upper()
        self.retries = max(1, retries)
        self.timeout = timeout
        self.sleeper = sleeper
        self.min_interval = max(0, min_interval)
        self._last_request_at = 0.0

    def lookup(self, manufacturer_part_number: str, manufacturer: str) -> IcecatLookupResult:
        if not manufacturer_part_number.strip() or not manufacturer.strip():
            return IcecatLookupResult("invalid", error="manufacturer and part number are required")

        params = {
            "prod_id": manufacturer_part_number.strip(),
            "vendor": manufacturer.strip(),
            "lang": self.language,
            "output": "productxml",
        }
        response = None
        for attempt in range(self.retries):
            try:
                wait_for = self.min_interval - (time.monotonic() - self._last_request_at)
                if wait_for > 0:
                    self.sleeper(wait_for)
                response = self.session.get(PRODUCT_URL, params=params, timeout=self.timeout)
                self._last_request_at = time.monotonic()
                if response.status_code in (401, 403):
                    raise IcecatAuthenticationError("Icecat rejected the configured credentials")
                response.raise_for_status()
                return self.parse_response(response.text, params=params)
            except IcecatAuthenticationError:
                raise
            except requests.RequestException:
                if attempt == self.retries - 1:
                    raise
                self.sleeper(2 ** attempt)
        return IcecatLookupResult("error", error="request failed")

    @staticmethod
    def parse_response(xml_text: str, *, params: dict, observed_at: str | None = None) -> IcecatLookupResult:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as error:
            return IcecatLookupResult("invalid_response", error=f"invalid XML: {error}", raw_xml=xml_text)

        product = root.find(".//Product")
        if product is None:
            return IcecatLookupResult("invalid_response", error="missing Product element", raw_xml=xml_text)

        code = product.attrib.get("Code", "")
        error_message = product.attrib.get("ErrorMessage", "").strip()
        lowered_error = error_message.lower()
        if code != "1":
            if "login" in lowered_error or "credential" in lowered_error or "authentication" in lowered_error:
                raise IcecatAuthenticationError("Icecat rejected the configured credentials")
            if "full icecat" in lowered_error or "not allowed" in lowered_error:
                status = "restricted"
            elif "not found" in lowered_error or code == "0":
                status = "not_found"
            else:
                status = "unavailable"
            return IcecatLookupResult(status, error=error_message or f"Icecat product code {code}", raw_xml=xml_text)

        supplier = product.find("./Supplier")
        category_name = product.find("./Category/Name")
        description = product.find("./ProductDescription")
        specifications = {}
        for product_feature in product.findall(".//ProductFeature"):
            feature_name = product_feature.find("./Feature/Name")
            name = feature_name.attrib.get("Value", "") if feature_name is not None else ""
            value = product_feature.attrib.get("Presentation_Value") or product_feature.attrib.get("Value", "")
            _put_feature(specifications, _feature_key(name), value.strip())

        source_record_url = f"{PRODUCT_URL}?{urlencode(params)}"
        observation = CatalogObservation(
            schema_version="1.0",
            source="open_icecat",
            source_tier="open",
            source_product_id=product.attrib.get("ID", ""),
            manufacturer=supplier.attrib.get("Name", params["vendor"]) if supplier is not None else params["vendor"],
            # The lookup identifier is the verified catalog MPN. Icecat's
            # Product@Prod_id can instead be a regional/distributor SKU, so
            # retain it as separate evidence rather than replacing identity.
            manufacturer_part_number=params["prod_id"],
            source_reported_part_number=product.attrib.get("Prod_id", ""),
            name=product.attrib.get("GeneratedLocalTitle") or product.attrib.get("Title") or product.attrib.get("Name", ""),
            category=category_name.attrib.get("Value", "") if category_name is not None else "",
            gtins=sorted({node.attrib["EAN"] for node in product.findall(".//EANCode") if node.attrib.get("EAN")}),
            image_url=product.attrib.get("HighPic") or product.attrib.get("Pic500x500", ""),
            manufacturer_url=description.attrib.get("URL", "") if description is not None else "",
            source_record_url=source_record_url,
            specifications=specifications,
            observed_at=observed_at or datetime.now(timezone.utc).isoformat(),
            raw_sha256=hashlib.sha256(xml_text.encode("utf-8")).hexdigest(),
        )
        return IcecatLookupResult("available", observation=observation, raw_xml=xml_text)
