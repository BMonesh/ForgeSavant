import sys
from pathlib import Path
import unittest

import pandas as pd


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from import_to_mongo import apply_verified_identity, component_documents, transform_processor  # noqa: E402


class ImportIdentityTests(unittest.TestCase):
    def test_transform_preserves_verified_manufacturer_identity(self):
        row = pd.Series({
            "name": "AMD Ryzen 5 5600X",
            "manufacturer": "AMD",
            "cores": 6,
            "threads": 12,
            "socket": "AM4",
            "price": 14999,
            "manufacturer_part_number": "100-100000065BOX",
            "manufacturer_part_number_source_url": "https://www.amd.com/example",
        })
        document = transform_processor(row)
        self.assertEqual(document["identity"]["manufacturerPartNumber"], "100-100000065BOX")
        self.assertEqual(document["identity"]["manufacturerPartNumberSourceUrl"], "https://www.amd.com/example")

    def test_verified_manifest_replaces_generic_catalog_identity(self):
        row = pd.Series({
            "name": "AMD Radeon RX 7600",
            "manufacturer": "AMD",
            "core_count": 2048,
            "memory": "8GB GDDR6",
            "tdp": "165W",
            "price": 24999,
        })
        from import_to_mongo import transform_gpu
        document = apply_verified_identity("gpus", transform_gpu(row))
        self.assertEqual(document["name"], "SAPPHIRE PULSE Radeon RX 7600 8GB")
        self.assertEqual(document["identity"]["manufacturerPartNumber"], "11324-01-20G")
        self.assertEqual(document["specifications"]["tdp"], "185W")

    def test_component_documents_collapses_duplicate_processor_aliases(self):
        documents, input_rows = component_documents("processors")
        self.assertEqual(input_rows, 14)
        self.assertEqual(len(documents), 13)
        part_numbers = [document["identity"]["manufacturerPartNumber"] for document in documents]
        self.assertEqual(len(part_numbers), len(set(part_numbers)))
        processor = next(document for document in documents if document["identity"]["manufacturerPartNumber"] == "BX8071512400F")
        self.assertEqual(processor["price"], 12499)


if __name__ == "__main__":
    unittest.main()
