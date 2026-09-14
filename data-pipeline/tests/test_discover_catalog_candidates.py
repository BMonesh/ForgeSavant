import json
import sys
from pathlib import Path
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from discover_catalog_candidates import (  # noqa: E402
    candidate_identity,
    derive_field,
    discover,
    looks_like_part_number,
    platform_from_chipset,
)
from test_retailer_scraper import build_session  # noqa: E402


class DerivationTests(unittest.TestCase):
    """A value is taken only when the product's own name states exactly one."""

    def test_reads_the_memory_generation_from_the_name(self):
        self.assertEqual(derive_field("ram", "G.SKILL Trident Z5 32GB DDR5-6000 CL30"), ("ram_type", "DDR5"))
        self.assertEqual(derive_field("ram", "CORSAIR VENGEANCE LPX 16GB DDR4-3200"), ("ram_type", "DDR4"))

    def test_rejects_a_name_stating_two_generations(self):
        self.assertIsNone(derive_field("ram", "Compare DDR4 and DDR5 memory kits"))

    def test_reads_the_storage_interface(self):
        self.assertEqual(derive_field("storage", "WD Blue SN580 1TB NVMe M.2 SSD"), ("interface", "NVMe"))
        self.assertEqual(derive_field("storage", "Crucial BX500 1TB SATA SSD"), ("interface", "SATA"))

    def test_rejects_a_drive_naming_both_interfaces(self):
        self.assertIsNone(derive_field("storage", "Samsung 990 EVO NVMe with SATA adapter"))

    def test_normalises_wattage(self):
        self.assertEqual(derive_field("power_supplies", "Cooler Master MWE Gold 1050 Watt V2"), ("wattage", "1050W"))
        self.assertEqual(derive_field("power_supplies", "Corsair RM750e 750W 80 Plus Gold"), ("wattage", "750W"))

    def test_rejects_a_supply_without_a_stated_wattage(self):
        self.assertIsNone(derive_field("power_supplies", "Antec CSK Series SMPS"))

    def test_rejects_a_case_listing_a_range_of_board_sizes(self):
        """"Supports ATX / Micro-ATX / Mini-ITX" states a range, not a value."""
        self.assertIsNone(derive_field("cabinets", "Lian Li O11 supports E-ATX, ATX, Micro-ATX, Mini-ITX"))

    def test_canonicalises_board_support(self):
        self.assertEqual(derive_field("cabinets", "NZXT H5 Flow Micro-ATX Case"), ("motherboard_support", "Micro-ATX"))
        self.assertEqual(derive_field("cabinets", "Cooler Master Shark X Mini ITX Case"), ("motherboard_support", "Mini-ITX"))


class ChipsetPlatformTests(unittest.TestCase):
    def test_derives_socket_and_memory_from_the_chipset(self):
        self.assertEqual(
            platform_from_chipset("B550"),
            {"chipset": "B550", "socket": "AM4", "memory_type": "DDR4"},
        )
        self.assertEqual(
            platform_from_chipset("X670E"),
            {"chipset": "X670E", "socket": "AM5", "memory_type": "DDR5"},
        )

    def test_a_micro_atx_suffix_states_the_board_size(self):
        self.assertEqual(platform_from_chipset("B760M")["form_factor"], "Micro-ATX")

    def test_omits_memory_where_the_chipset_supports_both(self):
        """LGA 1700 boards ship in DDR4 and DDR5 variants, so it is not implied."""
        self.assertNotIn("memory_type", platform_from_chipset("Z790"))

    def test_an_unknown_chipset_yields_only_itself(self):
        self.assertEqual(platform_from_chipset("Q999"), {"chipset": "Q999"})


class IdentityTests(unittest.TestCase):
    """Identifiers are read from the page, never invented from a slug."""

    def test_rejects_marketing_words_as_a_part_number(self):
        for value in ("CORSAIR-4000D-AIRFLOW", "cooler-master", "mid-tower", "32GB", "3200MHZ", "750W"):
            self.assertFalse(looks_like_part_number(value), value)

    def test_accepts_a_real_part_number(self):
        for value in ("CC-9011200-WW", "ST2000DM008", "F4-3600C18D-16GVK"):
            self.assertTrue(looks_like_part_number(value), value)

    def test_prefers_a_stated_part_number(self):
        identity = candidate_identity({"mpn": "CC-9011200-WW", "sku": "CORSAIR CASE", "gtin": "4718466015624"})
        self.assertEqual(identity["manufacturerPartNumber"], "CC-9011200-WW")
        self.assertEqual(identity["identitySource"], "page mpn")

    def test_falls_back_to_a_gtin(self):
        identity = candidate_identity({"mpn": "", "sku": "CORSAIR CASE", "gtin": "4718466015624"})
        self.assertEqual(identity["gtin"], "4718466015624")
        self.assertEqual(identity["manufacturerPartNumber"], "")

    def test_a_descriptive_sku_yields_no_identifier(self):
        identity = candidate_identity({"mpn": "", "sku": "SEAGATE BARRACUDA 2TB", "gtin": ""})
        self.assertEqual(identity["identitySource"], "none")
        self.assertEqual(identity["manufacturerPartNumber"], "")


def product_page(name, price="5000", sku="", gtin=None):
    node = {
        "@type": "Product",
        "name": name,
        "sku": sku,
        "offers": {"price": price, "priceCurrency": "INR", "availability": "https://schema.org/InStock"},
    }
    if gtin:
        node["gtin13"] = gtin
    return '<script type="application/ld+json">' + json.dumps(node) + "</script>"


class DiscoveryTests(unittest.TestCase):
    SITEMAP = "https://mdcomputers.in/feed_products.xml"
    GOOD = "https://mdcomputers.in/product/nzxt-h5-flow-micro-atx-cabinet"
    RANGE = "https://mdcomputers.in/product/lian-li-o11-atx-cabinet"
    PRICEY = "https://mdcomputers.in/product/bundle-mini-itx-cabinet"

    def _pages(self):
        locs = "".join(f"<url><loc><![CDATA[{u}]]></loc></url>" for u in (self.GOOD, self.RANGE, self.PRICEY))
        return {
            self.SITEMAP: f"<urlset>{locs}</urlset>",
            self.GOOD: product_page("NZXT H5 Flow Micro-ATX Case", "6500", gtin="4718466015624"),
            self.RANGE: product_page("Lian Li O11 supports ATX, Micro-ATX, Mini-ITX", "12000"),
            self.PRICEY: product_page("Shark X Mini ITX Case with SMPS, CPU Cooler", "360000", gtin="4719512157176"),
        }

    def test_accepts_only_products_whose_name_states_one_value(self):
        _, session = build_session(self._pages())
        report = discover("mdcomputers_in", session, {"cabinets": 10})
        names = [c["name"] for c in report["candidates"]["cabinets"]]
        self.assertIn("NZXT H5 Flow Micro-ATX Case", names)
        self.assertNotIn("Lian Li O11 supports ATX, Micro-ATX, Mini-ITX", names)
        self.assertEqual(report["rejections"]["cabinets"]["name does not state exactly one value"], 1)

    def test_flags_a_price_far_outside_the_category(self):
        """A 360,000 'case' is a bundle; it is surfaced rather than dropped."""
        _, session = build_session(self._pages())
        report = discover("mdcomputers_in", session, {"cabinets": 10})
        flagged = [c for c in report["candidates"]["cabinets"] if c["priceOutlier"]]
        self.assertEqual([c["price"] for c in flagged], [360000.0])
        self.assertEqual(report["priceOutliers"]["cabinets"], 1)

    def test_stops_once_the_target_is_reached(self):
        http, session = build_session(self._pages())
        discover("mdcomputers_in", session, {"cabinets": 1})
        fetched = [u for u in http.requested if "/product/" in u]
        self.assertEqual(len(fetched), 1)

    def test_records_the_name_each_specification_came_from(self):
        _, session = build_session(self._pages())
        report = discover("mdcomputers_in", session, {"cabinets": 10})
        row = report["candidates"]["cabinets"][0]
        self.assertEqual(row["derivedFrom"], row["name"])
        self.assertEqual(row["specifications"], {"motherboard_support": "Micro-ATX"})


if __name__ == "__main__":
    unittest.main()
