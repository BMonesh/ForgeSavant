import sys
from pathlib import Path
import unittest


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from connectors.blender_open_data import AggregateResult, match_verified_device, normalized_device  # noqa: E402


class BlenderOpenDataTests(unittest.TestCase):
    def test_matches_exact_gpu_model_without_laptop_ti_or_super_variants(self):
        results = [
            AggregateResult("NVIDIA GeForce RTX 4060", 100, 20, "https://example.test/query"),
            AggregateResult("NVIDIA GeForce RTX 4060 Laptop GPU", 90, 50, "https://example.test/query"),
            AggregateResult("NVIDIA GeForce RTX 4060 Ti", 120, 40, "https://example.test/query"),
        ]
        match = match_verified_device(
            {"currentName": "NVIDIA GeForce RTX 4060", "name": "ASUS RTX 4060"},
            results,
            "gpus",
        )
        self.assertEqual(match.device_name, "NVIDIA GeForce RTX 4060")

    def test_matches_cpu_with_generation_or_core_suffix(self):
        results = [
            AggregateResult("13th Gen Intel Core i7-13700K", 400, 16, "https://example.test/query"),
        ]
        match = match_verified_device({"name": "Intel Core i7-13700K"}, results, "processors")
        self.assertEqual(match.median_score, 400)

    def test_normalization_collapses_whitespace_and_symbols(self):
        self.assertEqual(normalized_device("AMD  Radeon RX-7800 XT"), "amd radeon rx 7800 xt")


if __name__ == "__main__":
    unittest.main()
