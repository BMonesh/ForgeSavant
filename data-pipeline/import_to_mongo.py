"""
ForgeSavant MongoDB Importer
=============================
Imports cleaned CSV data into MongoDB collections, matching the
existing Mongoose schema definitions in /models.

Features:
- Transforms cleaned CSV rows into MongoDB document format
- Maps flat CSV columns to nested specification objects
- Handles upserts (update existing or insert new)
- Validates data against expected schema before import
- Generates import summary with success/error counts

Usage:
    python import_to_mongo.py --component processors --uri mongodb://localhost:27017/forgesavant
    python import_to_mongo.py --all --uri mongodb://localhost:27017/forgesavant
    python import_to_mongo.py --dry-run --all  (validate without importing)

Dependencies:
    pip install pandas pymongo
"""

import argparse
import os
import json
import logging
from datetime import datetime

import pandas as pd

# ── Config ────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLEANED_DIR = os.path.join(BASE_DIR, "cleaned_data")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
#  SCHEMA TRANSFORMERS
#  These map flat CSV columns -> nested MongoDB document format
#  matching the Mongoose models in /models/*.model.js
# ══════════════════════════════════════════════════════════════════

def transform_processor(row: pd.Series) -> dict:
    """
    Transform processor CSV row to MongoDB document.
    Target schema: /models/processor.model.js

    CSV columns -> Nested Mongo document:
        cores, threads, base_clock, boost_clock, cache, socket, tdp
        -> specifications: { cores, threads, base_clock, ... }
    """
    return {
        "name": str(row["name"]),
        "type": str(row.get("type", "Desktop")),
        "manufacturer": str(row["manufacturer"]),
        "specifications": {
            "cores": int(row["cores"]) if pd.notna(row.get("cores")) else None,
            "threads": int(row["threads"]) if pd.notna(row.get("threads")) else None,
            "base_clock": str(row.get("base_clock", "")),
            "boost_clock": str(row.get("boost_clock", "")),
            "cache": str(row.get("cache", "")),
            "socket": str(row.get("socket", "")),
            "tdp": str(row.get("tdp", "")),
        },
        "price": float(row["price"]),
    }


def transform_gpu(row: pd.Series) -> dict:
    """
    Transform GPU CSV row to MongoDB document.
    Target schema: /models/graphicsCard.model.js
    """
    return {
        "name": str(row["name"]),
        "type": str(row.get("type", "Desktop")),
        "manufacturer": str(row["manufacturer"]),
        "specifications": {
            "core_count": int(row["core_count"]) if pd.notna(row.get("core_count")) else None,
            "base_clock": str(row.get("base_clock", "")),
            "boost_clock": str(row.get("boost_clock", "")),
            "memory": str(row.get("memory", "")),
            "tdp": str(row.get("tdp", "")),
        },
        "price": float(row["price"]),
    }


def transform_motherboard(row: pd.Series) -> dict:
    """
    Transform motherboard CSV row to MongoDB document.
    Target schema: /models/motherboard.model.js
    """
    return {
        "name": str(row["name"]),
        "type": str(row.get("type", "Desktop")),
        "manufacturer": str(row["manufacturer"]),
        "specifications": {
            "socket": str(row.get("socket", "")),
            "chipset": str(row.get("chipset", "")),
            "form_factor": str(row.get("form_factor", "")),
            "memory_slots": int(row["memory_slots"]) if pd.notna(row.get("memory_slots")) else None,
            "max_memory": str(row.get("max_memory", "")),
            "pcie_slots": int(row["pcie_slots"]) if pd.notna(row.get("pcie_slots")) else None,
            "sata_ports": int(row["sata_ports"]) if pd.notna(row.get("sata_ports")) else None,
            "m2_slots": int(row["m2_slots"]) if pd.notna(row.get("m2_slots")) else None,
            "lan": str(row.get("lan", "")),
            "usb_ports": str(row.get("usb_ports", "")),
        },
        "price": float(row["price"]),
    }


def transform_ram(row: pd.Series) -> dict:
    """
    Transform RAM CSV row to MongoDB document.
    Target schema: /models/ram.model.js
    """
    return {
        "name": str(row["name"]),
        "type": str(row.get("type", "Desktop")),
        "manufacturer": str(row["manufacturer"]),
        "specifications": {
            "capacity": str(row.get("capacity", "")),
            "type": str(row.get("ram_type", "")),
            "speed": str(row.get("speed", "")),
            "cas_latency": int(row["cas_latency"]) if pd.notna(row.get("cas_latency")) else None,
            "voltage": str(row.get("voltage", "")),
            "rgb": bool(row.get("rgb", False)),
        },
        "price": float(row["price"]),
    }


# ══════════════════════════════════════════════════════════════════
#  VALIDATION
# ══════════════════════════════════════════════════════════════════

def validate_document(doc: dict, required_fields: list[str]) -> list[str]:
    """Validate a document has all required fields and they're not empty."""
    errors = []
    for field_path in required_fields:
        parts = field_path.split(".")
        value = doc
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                value = None
                break

        if value is None or (isinstance(value, str) and value.strip() == ""):
            errors.append(f"Missing or empty field: {field_path}")

    return errors


REQUIRED_FIELDS = {
    "processors": ["name", "manufacturer", "specifications.socket", "price"],
    "gpus": ["name", "manufacturer", "specifications.memory", "price"],
    "motherboards": ["name", "manufacturer", "specifications.socket", "specifications.chipset", "price"],
    "ram": ["name", "manufacturer", "specifications.capacity", "specifications.type", "price"],
}


# ══════════════════════════════════════════════════════════════════
#  IMPORT ENGINE
# ══════════════════════════════════════════════════════════════════

COMPONENT_CONFIG = {
    "processors": {
        "csv": os.path.join(CLEANED_DIR, "processors_cleaned.csv"),
        "collection": "processors",
        "transformer": transform_processor,
    },
    "gpus": {
        "csv": os.path.join(CLEANED_DIR, "gpus_cleaned.csv"),
        "collection": "graphiccards",
        "transformer": transform_gpu,
    },
    "motherboards": {
        "csv": os.path.join(CLEANED_DIR, "motherboards_cleaned.csv"),
        "collection": "motherboards",
        "transformer": transform_motherboard,
    },
    "ram": {
        "csv": os.path.join(CLEANED_DIR, "ram_cleaned.csv"),
        "collection": "rams",
        "transformer": transform_ram,
    },
}


def dry_run_import(component: str) -> dict:
    """
    Validate and preview what would be imported without touching MongoDB.
    Useful for verifying data quality before actual import.
    """
    config = COMPONENT_CONFIG[component]
    csv_path = config["csv"]
    transformer = config["transformer"]
    required = REQUIRED_FIELDS.get(component, [])

    if not os.path.exists(csv_path):
        return {"error": f"File not found: {csv_path}"}

    df = pd.read_csv(csv_path)
    results = {
        "component": component,
        "collection": config["collection"],
        "total_rows": len(df),
        "valid": 0,
        "invalid": 0,
        "validation_errors": [],
        "sample_documents": [],
    }

    for idx, row in df.iterrows():
        try:
            doc = transformer(row)
            errors = validate_document(doc, required)
            if errors:
                results["invalid"] += 1
                results["validation_errors"].append({
                    "row": idx,
                    "name": row.get("name", "unknown"),
                    "errors": errors,
                })
            else:
                results["valid"] += 1
                # Include first 2 docs as samples
                if len(results["sample_documents"]) < 2:
                    results["sample_documents"].append(doc)
        except Exception as e:
            results["invalid"] += 1
            results["validation_errors"].append({
                "row": idx,
                "error": str(e),
            })

    return results


def import_to_mongodb(component: str, mongo_uri: str) -> dict:
    """
    Import cleaned CSV data into MongoDB collection.
    Uses upserts (update if exists, insert if new) based on component name.
    """
    try:
        from pymongo import MongoClient
    except ImportError:
        return {"error": "pymongo not installed. Run: pip install pymongo"}

    config = COMPONENT_CONFIG[component]
    csv_path = config["csv"]
    collection_name = config["collection"]
    transformer = config["transformer"]

    if not os.path.exists(csv_path):
        return {"error": f"File not found: {csv_path}"}

    df = pd.read_csv(csv_path)
    client = MongoClient(mongo_uri)
    db = client.get_default_database()
    collection = db[collection_name]

    results = {
        "component": component,
        "collection": collection_name,
        "inserted": 0,
        "updated": 0,
        "errors": 0,
        "error_details": [],
    }

    for _, row in df.iterrows():
        try:
            doc = transformer(row)
            # Upsert: update if name exists, insert if not
            result = collection.update_one(
                {"name": doc["name"]},
                {"$set": doc},
                upsert=True,
            )
            if result.upserted_id:
                results["inserted"] += 1
            else:
                results["updated"] += 1
        except Exception as e:
            results["errors"] += 1
            results["error_details"].append({
                "name": row.get("name", "unknown"),
                "error": str(e),
            })

    client.close()
    logger.info(
        f"Import complete for {component}: "
        f"{results['inserted']} inserted, "
        f"{results['updated']} updated, "
        f"{results['errors']} errors"
    )
    return results


# ══════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="ForgeSavant MongoDB Importer"
    )
    parser.add_argument(
        "--component",
        choices=list(COMPONENT_CONFIG.keys()),
        help="Component type to import",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Import all component types",
    )
    parser.add_argument(
        "--uri",
        default="mongodb://localhost:27017/forgesavant",
        help="MongoDB connection URI",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate data without importing to MongoDB",
    )

    args = parser.parse_args()

    components = []
    if args.all:
        components = list(COMPONENT_CONFIG.keys())
    elif args.component:
        components = [args.component]
    else:
        parser.print_help()
        return

    for component in components:
        if args.dry_run:
            result = dry_run_import(component)
            print(f"\n{'='*50}")
            print(f"DRY RUN: {component.upper()}")
            print(f"{'='*50}")
            print(f"  Target collection: {result.get('collection', 'N/A')}")
            print(f"  Total rows:        {result.get('total_rows', 0)}")
            print(f"  Valid:             {result.get('valid', 0)}")
            print(f"  Invalid:           {result.get('invalid', 0)}")

            if result.get("validation_errors"):
                print(f"\n  Validation Errors:")
                for err in result["validation_errors"][:5]:
                    print(f"    Row {err.get('row', '?')}: {err.get('name', '?')}")
                    for e in err.get("errors", []):
                        print(f"      - {e}")

            if result.get("sample_documents"):
                print(f"\n  Sample Document:")
                print(json.dumps(result["sample_documents"][0], indent=4))

            print()
        else:
            result = import_to_mongodb(component, args.uri)
            print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
