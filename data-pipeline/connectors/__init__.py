"""External-source connectors for the ForgeSavant catalog pipeline."""

from .flipkart_affiliate import FlipkartAffiliateConnector, RetailOffer, RetailerAuthenticationError
from .icecat import CatalogObservation, IcecatAuthenticationError, IcecatLookupResult, OpenIcecatConnector

__all__ = [
    "CatalogObservation",
    "FlipkartAffiliateConnector",
    "IcecatAuthenticationError",
    "IcecatLookupResult",
    "OpenIcecatConnector",
    "RetailOffer",
    "RetailerAuthenticationError",
]
