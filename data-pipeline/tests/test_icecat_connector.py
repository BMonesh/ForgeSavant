import sys
from pathlib import Path
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from connectors.icecat import IcecatAuthenticationError, OpenIcecatConnector  # noqa: E402


AVAILABLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<ICECAT-interface>
  <Product Code="1" ID="29900045" Prod_id="REGIONAL-SKU-1" Name="X4071UHSU-B1"
    GeneratedLocalTitle="iiyama ProLite X4071UHSU-B1 monitor" HighPic="https://images.example/product.jpg">
    <Supplier ID="80" Name="iiyama" />
    <Category ID="222"><Name Value="Computer Monitors" /></Category>
    <ProductDescription URL="https://manufacturer.example/product" />
    <EANCode EAN="4948570114344" />
    <ProductFeature Presentation_Value="3840 x 2160 pixels">
      <Feature><Name Value="Display resolution" /></Feature>
    </ProductFeature>
    <ProductFeature Presentation_Value="4K Ultra HD">
      <Feature><Name Value="HD type" /></Feature>
    </ProductFeature>
  </Product>
</ICECAT-interface>"""

RESTRICTED_XML = """<?xml version="1.0" encoding="UTF-8"?>
<ICECAT-interface><Product Code="-1" ID="?ABC?" ErrorMessage="You are not allowed to have Full Icecat access" /></ICECAT-interface>"""


class IcecatConnectorTests(unittest.TestCase):
    def test_parses_available_product_into_canonical_observation(self):
        params = {"prod_id": "X4071UHSU-B1", "vendor": "iiyama", "lang": "EN", "output": "productxml"}
        result = OpenIcecatConnector.parse_response(AVAILABLE_XML, params=params, observed_at="2026-07-22T00:00:00+00:00")

        self.assertEqual(result.status, "available")
        self.assertEqual(result.observation.source_product_id, "29900045")
        self.assertEqual(result.observation.manufacturer_part_number, "X4071UHSU-B1")
        self.assertEqual(result.observation.source_reported_part_number, "REGIONAL-SKU-1")
        self.assertEqual(result.observation.gtins, ["4948570114344"])
        self.assertEqual(result.observation.specifications["display_resolution"], "3840 x 2160 pixels")
        self.assertEqual(len(result.observation.raw_sha256), 64)
        self.assertNotIn("password", result.observation.source_record_url.lower())

    def test_classifies_full_catalog_only_product_as_restricted(self):
        params = {"prod_id": "ABC", "vendor": "Example", "lang": "EN", "output": "productxml"}
        result = OpenIcecatConnector.parse_response(RESTRICTED_XML, params=params)
        self.assertEqual(result.status, "restricted")
        self.assertIsNone(result.observation)

    def test_http_authentication_failure_is_not_retried(self):
        class Response:
            status_code = 401
            text = ""

            def raise_for_status(self):
                raise AssertionError("authentication should be handled first")

        class Session:
            def __init__(self):
                self.headers = {}
                self.auth = None

            def get(self, *_args, **_kwargs):
                return Response()

        connector = OpenIcecatConnector("user", "secret", session=Session(), sleeper=lambda _seconds: None, min_interval=0)
        with self.assertRaises(IcecatAuthenticationError):
            connector.lookup("ABC", "Example")

    def test_invalid_xml_is_quarantined_as_invalid_response(self):
        params = {"prod_id": "ABC", "vendor": "Example", "lang": "EN", "output": "productxml"}
        result = OpenIcecatConnector.parse_response("not XML", params=params)
        self.assertEqual(result.status, "invalid_response")


if __name__ == "__main__":
    unittest.main()
