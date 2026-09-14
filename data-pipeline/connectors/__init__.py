"""External-source connectors for the ForgeSavant catalog pipeline."""

from .flipkart_affiliate import FlipkartAffiliateConnector, RetailOffer, RetailerAuthenticationError
from .icecat import CatalogObservation, IcecatAuthenticationError, IcecatLookupResult, OpenIcecatConnector
from .retailer_scraper import (
    ADAPTERS,
    AmazonIndiaAdapter,
    CatalogTarget,
    MDComputersAdapter,
    PrimeABGBAdapter,
    RetailerPageUnavailable,
    RobotsDisallowed,
    ScraperSession,
)

__all__ = [
    "ADAPTERS",
    "AmazonIndiaAdapter",
    "CatalogObservation",
    "CatalogTarget",
    "MDComputersAdapter",
    "PrimeABGBAdapter",
    "RetailerPageUnavailable",
    "RobotsDisallowed",
    "ScraperSession",
    "FlipkartAffiliateConnector",
    "IcecatAuthenticationError",
    "IcecatLookupResult",
    "OpenIcecatConnector",
    "RetailOffer",
    "RetailerAuthenticationError",
]
