import json
import sys
from pathlib import Path
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from connectors.flipkart_affiliate import FlipkartAffiliateConnector, RetailerAuthenticationError  # noqa: E402
from sync_retailer import feed_row, match_offer, title_tokens  # noqa: E402


FIXTURE = {
    "productInfoList": [{
        "productBaseInfoV1": {
            "productId": "CPU123",
            "title": "AMD Ryzen 5 5600X Desktop Processor",
            "flipkartSellingPrice": {"amount": 12999, "currency": "INR"},
            "flipkartSpecialPrice": {"amount": 12499, "currency": "INR"},
            "productUrl": "https://www.flipkart.com/example",
            "imageUrls": {"400x400": "http://example.test/cpu.jpg"},
        },
        "productShippingInfoV1": {"inStock": True},
    }]
}


class RetailerSyncTests(unittest.TestCase):
    def test_parses_documented_flipkart_shape(self):
        offers = FlipkartAffiliateConnector.parse_response(FIXTURE, "2026-07-20T00:00:00+00:00")
        self.assertEqual(len(offers), 1)
        self.assertEqual(offers[0].price, 12499)
        self.assertEqual(offers[0].availability, "in_stock")
        self.assertTrue(offers[0].image_url.startswith("https://"))

    def test_matches_model_name_but_rejects_accessory(self):
        offer = FlipkartAffiliateConnector.parse_response(FIXTURE)[0]
        self.assertEqual(match_offer("AMD Ryzen 5 5600X", [offer]), offer)
        self.assertIsNone(match_offer("AMD Ryzen 7 7800X3D", [offer]))

    def test_tokenizer_removes_generic_product_words(self):
        self.assertEqual(title_tokens("AMD Ryzen 5 5600X Desktop Processor"), {"amd", "ryzen", "5600x"})

    def test_exports_the_signed_admin_feed_contract(self):
        offer = FlipkartAffiliateConnector.parse_response(FIXTURE, "2026-07-20T00:00:00+00:00")[0]
        row = feed_row("processors", offer)
        self.assertEqual(row["category"], "processors")
        self.assertEqual(row["source_item_id"], "CPU123")
        self.assertEqual(row["observed_at"], "2026-07-20T00:00:00+00:00")
        self.assertNotIn("data_status", row)

    def test_authentication_failure_is_not_retried_as_a_product_error(self):
        class Response:
            status_code = 401
            def raise_for_status(self):
                raise AssertionError("authentication should be handled first")

        class Session:
            def __init__(self):
                self.headers = {}
            def get(self, *_args, **_kwargs):
                return Response()

        connector = FlipkartAffiliateConnector("id", "token", session=Session(), sleeper=lambda _seconds: None, min_interval=0)
        with self.assertRaises(RetailerAuthenticationError):
            connector.search("Ryzen 5 5600X")


if __name__ == "__main__":
    unittest.main()
