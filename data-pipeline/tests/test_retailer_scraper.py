import json
import sys
from pathlib import Path
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from connectors.retailer_scraper import (  # noqa: E402
    AmazonIndiaAdapter,
    CatalogTarget,
    MDComputersAdapter,
    PrimeABGBAdapter,
    conflicting_identifier,
    offer_price,
    RetailerPageUnavailable,
    RobotsDisallowed,
    ScraperSession,
    extract_product_offer,
    normalize_availability,
    normalize_identifier,
    parse_price,
    slug_identifiers,
)
from scrape_retailers import (  # noqa: E402
    feed_row,
    load_identifier_map,
    load_targets,
    price_disagreements,
)


def product_page(price="15100.00", availability="https://schema.org/InStock", sku="MD-SKU-1"):
    return (
        '<html><head><script type="application/ld+json">'
        + json.dumps({
            "@context": "https://schema.org",
            "@type": "Product",
            "name": "AMD Ryzen 5 5600X Processor",
            "sku": sku,
            "image": ["https://cdn.example/img.jpg"],
            "offers": {
                "@type": "Offer",
                "price": price,
                "priceCurrency": "INR",
                "availability": availability,
            },
        })
        + "</script></head><body></body></html>"
    )


class FakeResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code


class FakeHttp:
    """Stands in for requests.Session; records every URL fetched."""

    def __init__(self, pages, robots="User-agent: *\nAllow: /\n"):
        self.pages = pages
        self.robots = robots
        self.headers = {}
        self.requested = []

    def get(self, url, timeout=None):
        self.requested.append(url)
        if url.endswith("/robots.txt"):
            return FakeResponse(self.robots, 200 if self.robots is not None else 404)
        if url in self.pages:
            return FakeResponse(self.pages[url])
        return FakeResponse("not found", 404)


def build_session(pages, robots="User-agent: *\nAllow: /\n"):
    http = FakeHttp(pages, robots)
    session = ScraperSession(session=http, sleeper=lambda _seconds: None, min_delay=0, max_delay=0)
    return http, session


class SlugMatchingTests(unittest.TestCase):
    """A part number that is a prefix of another must never match it."""

    def test_does_not_match_a_longer_variant_part_number(self):
        candidates = slug_identifiers("intel-i5-13600kf-bx8071513600kf-desktop-processor")
        self.assertNotIn(normalize_identifier("BX8071513600K"), candidates)
        self.assertIn(normalize_identifier("BX8071513600KF"), candidates)

    def test_matches_a_part_number_split_across_separators(self):
        candidates = slug_identifiers("amd-ryzen-5-5600x-100-100000065box-desktop-processor")
        self.assertIn(normalize_identifier("100-100000065BOX"), candidates)

    def test_matches_a_part_number_at_the_end_of_the_slug(self):
        candidates = slug_identifiers("amd-ryzen-7-5800x3d-desktop-processor-100-100000651pof")
        self.assertIn(normalize_identifier("100-100000651POF"), candidates)

    def test_does_not_match_a_suffixed_variant(self):
        self.assertNotIn(
            normalize_identifier("BX8071512900K"),
            slug_identifiers("intel-i9-12900ks-bx8071512900ks-desktop-processor"),
        )


class ParsingTests(unittest.TestCase):
    def test_parses_indian_price_formatting(self):
        self.assertEqual(parse_price("₹15,100.00"), 15100.0)
        self.assertEqual(parse_price("15100"), 15100.0)
        self.assertIsNone(parse_price(""))
        self.assertIsNone(parse_price("0"))
        self.assertIsNone(parse_price("out of stock"))

    def test_maps_schema_availability_to_store_vocabulary(self):
        self.assertEqual(normalize_availability("https://schema.org/InStock"), "in_stock")
        self.assertEqual(normalize_availability("http://schema.org/OutOfStock"), "out_of_stock")
        self.assertEqual(normalize_availability("PreOrder"), "preorder")
        self.assertEqual(normalize_availability("something else"), "unknown")

    def test_extracts_an_offer_from_product_markup(self):
        offer = extract_product_offer(product_page())
        self.assertEqual(offer["price"], 15100.0)
        self.assertEqual(offer["currency"], "INR")
        self.assertEqual(offer["availability"], "in_stock")
        self.assertEqual(offer["sku"], "MD-SKU-1")

    def test_reads_products_nested_in_a_graph(self):
        html = (
            '<script type="application/ld+json">'
            + json.dumps({"@graph": [
                {"@type": "WebPage"},
                {"@type": "Product", "name": "X", "offers": {"price": "999", "priceCurrency": "INR"}},
            ]})
            + "</script>"
        )
        self.assertEqual(extract_product_offer(html)["price"], 999.0)

    def test_ignores_a_product_without_a_usable_price(self):
        html = (
            '<script type="application/ld+json">'
            + json.dumps({"@type": "Product", "name": "X", "offers": {"priceCurrency": "INR"}})
            + "</script>"
        )
        self.assertIsNone(extract_product_offer(html))

    def test_ignores_malformed_json_blocks(self):
        self.assertIsNone(extract_product_offer('<script type="application/ld+json">{ broken</script>'))


class ScraperSessionTests(unittest.TestCase):
    def test_refuses_a_path_disallowed_by_robots(self):
        _, session = build_session({}, robots="User-agent: *\nDisallow: /product/\n")
        with self.assertRaises(RobotsDisallowed):
            session.get("https://example.test/product/thing")

    def test_allows_a_path_outside_the_disallowed_prefix(self):
        http, session = build_session(
            {"https://example.test/catalog": "ok"}, robots="User-agent: *\nDisallow: /product/\n"
        )
        self.assertEqual(session.get("https://example.test/catalog"), "ok")

    def test_fetches_robots_once_per_host(self):
        http, session = build_session({"https://example.test/a": "a", "https://example.test/b": "b"})
        session.get("https://example.test/a")
        session.get("https://example.test/b")
        self.assertEqual(sum(1 for url in http.requested if url.endswith("robots.txt")), 1)

    def test_raises_for_a_non_200_response(self):
        _, session = build_session({})
        with self.assertRaises(RetailerPageUnavailable):
            session.get("https://example.test/missing")


class MDComputersAdapterTests(unittest.TestCase):
    SITEMAP = "https://mdcomputers.in/feed_products.xml"

    def _sitemap(self, *slugs):
        locs = "".join(f"<url><loc><![CDATA[https://mdcomputers.in/product/{slug}]]></loc></url>" for slug in slugs)
        return f"<urlset>{locs}</urlset>"

    def test_matches_exactly_one_url_per_part_number(self):
        pages = {
            self.SITEMAP: self._sitemap(
                "amd-ryzen-5-5600x-100-100000065box-desktop-processor",
                "intel-i5-13600k-bx8071513600k-desktop-processor",
            ),
        }
        _, session = build_session(pages)
        targets = [
            CatalogTarget("processors", "AMD Ryzen 5 5600X", "100-100000065BOX"),
            CatalogTarget("processors", "Intel Core i5-13600K", "BX8071513600K"),
        ]
        discovery = MDComputersAdapter(session).discover(targets)
        self.assertEqual(set(discovery.matches), {"100-100000065BOX", "BX8071513600K"})
        self.assertEqual(discovery.ambiguous, {})

    def test_reports_ambiguity_instead_of_guessing(self):
        pages = {
            self.SITEMAP: self._sitemap(
                "asus-prime-b550m-a-motherboard",
                "asus-prime-b550m-a-csm-motherboard",
            ),
        }
        _, session = build_session(pages)
        targets = [CatalogTarget("motherboards", "ASUS PRIME B550M-A", "PRIME-B550M-A")]
        discovery = MDComputersAdapter(session).discover(targets)
        self.assertEqual(discovery.matches, {})
        self.assertEqual(len(discovery.ambiguous["PRIME-B550M-A"]), 2)

    def test_does_not_confuse_a_variant_part_number(self):
        pages = {self.SITEMAP: self._sitemap("intel-i5-13600kf-bx8071513600kf-desktop-processor")}
        _, session = build_session(pages)
        discovery = MDComputersAdapter(session).discover(
            [CatalogTarget("processors", "Intel Core i5-13600K", "BX8071513600K")]
        )
        self.assertEqual(discovery.matches, {})
        self.assertEqual(discovery.ambiguous, {})

    def test_builds_an_offer_carrying_retailer_provenance(self):
        url = "https://mdcomputers.in/product/amd-ryzen-5-5600x-100-100000065box-desktop-processor"
        _, session = build_session({url: product_page()})
        target = CatalogTarget("processors", "AMD Ryzen 5 5600X", "100-100000065BOX")
        offer = MDComputersAdapter(session).fetch_offer(url, target, collected_at="2026-09-02T00:00:00+00:00")
        self.assertEqual(offer.source, "mdcomputers_in")
        self.assertEqual(offer.price, 15100.0)
        self.assertEqual(offer.source_url, url)
        self.assertEqual(offer.source_item_id, "MD-SKU-1")

    def test_a_page_without_product_markup_is_an_error_not_a_zero_price(self):
        url = "https://mdcomputers.in/product/x"
        _, session = build_session({url: "<html><body>no markup</body></html>"})
        with self.assertRaises(RetailerPageUnavailable):
            MDComputersAdapter(session).fetch_offer(
                url, CatalogTarget("processors", "X", "X-1"), collected_at="2026-09-02T00:00:00+00:00"
            )


class AmazonAdapterTests(unittest.TestCase):
    def test_does_not_discover_anything_without_an_operator_supplied_asin(self):
        _, session = build_session({})
        discovery = AmazonIndiaAdapter(session).discover(
            [CatalogTarget("processors", "AMD Ryzen 5 5600X", "100-100000065BOX")]
        )
        self.assertEqual(discovery.matches, {})
        self.assertEqual(discovery.ambiguous, {})

    def test_never_requests_a_page_during_discovery(self):
        http, session = build_session({})
        AmazonIndiaAdapter(session).discover([CatalogTarget("processors", "X", "X-1")])
        self.assertEqual(http.requested, [])

    def test_builds_a_product_url_from_a_mapped_asin(self):
        _, session = build_session({})
        discovery = AmazonIndiaAdapter(session).discover([
            CatalogTarget("processors", "AMD Ryzen 5 5600X", "100-100000065BOX", identifiers=("B08166SLDF",)),
        ])
        self.assertEqual(discovery.matches["100-100000065BOX"], "https://www.amazon.in/dp/B08166SLDF")

    def test_rejects_an_identifier_that_is_not_an_asin(self):
        _, session = build_session({})
        discovery = AmazonIndiaAdapter(session).discover([
            CatalogTarget("processors", "X", "X-1", identifiers=("not-an-asin",)),
        ])
        self.assertEqual(discovery.matches, {})

    def test_reports_a_bot_challenge_rather_than_inventing_a_price(self):
        url = "https://www.amazon.in/dp/B08166SLDF"
        _, session = build_session({url: "<html><body>Enter the characters you see below</body></html>"})
        with self.assertRaisesRegex(RetailerPageUnavailable, "bot challenge"):
            AmazonIndiaAdapter(session).fetch_offer(
                url, CatalogTarget("processors", "X", "X-1"), collected_at="2026-09-02T00:00:00+00:00"
            )

    def test_reads_the_asin_back_out_of_the_url(self):
        self.assertEqual(AmazonIndiaAdapter.identifier_from_url("https://www.amazon.in/dp/B08166SLDF"), "B08166SLDF")


class FeedTests(unittest.TestCase):
    def test_feed_row_carries_the_part_number_for_exact_admin_matching(self):
        from connectors.retailer_scraper import RetailOffer

        offer = RetailOffer(
            source_item_id="MD-SKU-1", name="AMD Ryzen 5 5600X", price=15100.0, currency="INR",
            availability="in_stock", source="mdcomputers_in",
            source_url="https://mdcomputers.in/product/x", image_url="https://cdn.example/i.jpg",
            collected_at="2026-09-02T00:00:00+00:00",
        )
        row = feed_row(CatalogTarget("processors", "AMD Ryzen 5 5600X", "100-100000065BOX"), offer)
        self.assertEqual(row["manufacturer_part_number"], "100-100000065BOX")
        self.assertEqual(row["category"], "processors")
        self.assertEqual(row["observed_at"], "2026-09-02T00:00:00+00:00")

    def test_loads_identifier_map_keyed_by_part_number(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "asins.json"
            path.write_text(json.dumps([{"manufacturerPartNumber": "cpu-1", "asin": "B08166SLDF"}]), encoding="utf-8")
            self.assertEqual(load_identifier_map(path), {"CPU-1": ["B08166SLDF"]})

    def test_targets_come_only_from_the_verified_identity_manifest(self):
        targets = load_targets(PIPELINE_DIR / "verified_identity")
        self.assertTrue(all(target.manufacturer_part_number for target in targets))
        self.assertEqual(len(targets), 58)



class PriceSpecificationTests(unittest.TestCase):
    """A list price sitting beside the selling price must never win."""

    def test_prefers_the_selling_price_over_the_list_price(self):
        offer = {"priceSpecification": [
            {"@type": "UnitPriceSpecification", "price": "4494", "priceCurrency": "INR"},
            {"@type": "UnitPriceSpecification", "price": "6444", "priceCurrency": "INR",
             "priceType": "https://schema.org/ListPrice"},
        ]}
        self.assertEqual(offer_price(offer), (4494.0, "INR"))

    def test_ignores_list_price_ordering(self):
        offer = {"priceSpecification": [
            {"price": "6444", "priceCurrency": "INR", "priceType": "https://schema.org/ListPrice"},
            {"price": "4494", "priceCurrency": "INR"},
        ]}
        self.assertEqual(offer_price(offer)[0], 4494.0)

    def test_a_direct_price_still_wins(self):
        self.assertEqual(offer_price({"price": "999", "priceCurrency": "INR"}), (999.0, "INR"))

    def test_a_single_specification_object_is_accepted(self):
        self.assertEqual(offer_price({"priceSpecification": {"price": "150", "priceCurrency": "INR"}})[0], 150.0)

    def test_an_offer_with_only_a_list_price_yields_nothing(self):
        offer = {"priceSpecification": [{"price": "6444", "priceType": "https://schema.org/ListPrice"}]}
        self.assertEqual(offer_price(offer), (None, ""))


class ConflictingIdentifierTests(unittest.TestCase):
    """The page's own data is a second opinion on what the slug claims."""

    def test_flags_a_sibling_part_number(self):
        product = {"name": "Seagate 2TB Barracuda ST2000DM004 256MB", "sku": "ST2000DM004", "mpn": ""}
        self.assertEqual(conflicting_identifier("ST2000DM008", product), "st2000dm004")

    def test_flags_a_suffixed_variant(self):
        product = {"name": "Intel Core i5-13600KF", "sku": "BX8071513600KF", "mpn": ""}
        self.assertEqual(conflicting_identifier("BX8071513600K", product), "bx8071513600kf")

    def test_a_page_confirming_the_part_number_is_not_a_conflict(self):
        product = {"name": "AMD Ryzen 5 5600X", "sku": "100-100000065BOX", "mpn": ""}
        self.assertIsNone(conflicting_identifier("100-100000065BOX", product))

    def test_a_descriptive_sku_is_not_treated_as_a_conflict(self):
        """Absence of confirmation must not discard an otherwise good offer."""
        product = {"name": "Seagate Barracuda 2TB 7200 RPM Hard Drive", "sku": "SEAGATE-BARRACUDA-2TB-7200", "mpn": ""}
        self.assertIsNone(conflicting_identifier("ST2000DM008", product))

    def test_a_model_name_part_number_is_confirmed_by_the_title(self):
        product = {"name": "Gigabyte B450M S2H Motherboard", "sku": "B450M S2H", "mpn": ""}
        self.assertIsNone(conflicting_identifier("B450M-S2H", product))

    def test_fetch_offer_refuses_a_page_that_identifies_itself_differently(self):
        url = "https://www.primeabgb.com/online-price-reviews-india/seagate-st2000dm008/"
        page = (
            '<script type="application/ld+json">'
            + json.dumps({"@type": "Product", "name": "Seagate 2TB Barracuda ST2000DM004",
                          "sku": "ST2000DM004",
                          "offers": {"price": "5399", "priceCurrency": "INR"}})
            + "</script>"
        )
        _, session = build_session({url: page})
        target = CatalogTarget("storage", "Seagate Barracuda 2TB", "ST2000DM008")
        with self.assertRaisesRegex(RetailerPageUnavailable, "identifies itself as st2000dm004"):
            PrimeABGBAdapter(session).fetch_offer(url, target, collected_at="2026-09-02T00:00:00+00:00")


class PrimeABGBAdapterTests(unittest.TestCase):
    INDEX = "https://www.primeabgb.com/sitemap_index.xml"

    def _index(self, *sitemaps):
        return "<sitemapindex>" + "".join(f"<sitemap><loc>{s}</loc></sitemap>" for s in sitemaps) + "</sitemapindex>"

    def _sitemap(self, *slugs):
        base = "https://www.primeabgb.com/online-price-reviews-india"
        return "<urlset>" + "".join(f"<url><loc>{base}/{s}/</loc></url>" for s in slugs) + "</urlset>"

    def test_follows_the_sitemap_index_to_the_product_sitemaps(self):
        one = "https://www.primeabgb.com/product-sitemap.xml"
        pages = {
            self.INDEX: self._index(one, "https://www.primeabgb.com/post-sitemap.xml"),
            one: self._sitemap("msi-mpg-z790-edge-wifi-ddr5-intel-motherboard"),
        }
        http, session = build_session(pages)
        discovery = PrimeABGBAdapter(session).discover(
            [CatalogTarget("motherboards", "MSI MPG Z790 EDGE WIFI", "MPG-Z790-EDGE-WIFI")]
        )
        self.assertIn("MPG-Z790-EDGE-WIFI", discovery.matches)
        # The post sitemap is not a product sitemap and must not be fetched.
        self.assertNotIn("https://www.primeabgb.com/post-sitemap.xml", http.requested)

    def test_declares_that_its_content_may_not_train_a_model(self):
        """robots.txt carries Content-Signal: ai-train=no."""
        self.assertFalse(PrimeABGBAdapter.permits_ai_training)
        self.assertTrue(MDComputersAdapter.permits_ai_training)


class PriceDisagreementTests(unittest.TestCase):
    def _offer(self, source, price, part="ST2000DM008"):
        return {"manufacturer_part_number": part, "source": source, "price": price, "availability": "in_stock"}

    def test_flags_a_wide_spread_between_retailers(self):
        flagged = price_disagreements([self._offer("a", 13770.0), self._offer("b", 5399.0)])
        self.assertEqual(len(flagged), 1)
        self.assertEqual(flagged[0]["quotes"][0]["price"], 5399.0)

    def test_ignores_ordinary_variation(self):
        self.assertEqual(price_disagreements([self._offer("a", 15999.0), self._offer("b", 17524.0)]), [])

    def test_ignores_a_product_only_one_retailer_lists(self):
        self.assertEqual(price_disagreements([self._offer("a", 100.0)]), [])

    def test_carries_the_training_permission_into_the_feed(self):
        from connectors.retailer_scraper import RetailOffer

        offer = RetailOffer(
            source_item_id="X", name="X", price=1.0, currency="INR", availability="in_stock",
            source="primeabgb_com", source_url="https://x", image_url="", collected_at="2026-09-02T00:00:00+00:00",
        )
        row = feed_row(CatalogTarget("storage", "X", "X-1"), offer, permits_ai_training=False)
        self.assertIs(row["ai_training_permitted"], False)

if __name__ == "__main__":
    unittest.main()
