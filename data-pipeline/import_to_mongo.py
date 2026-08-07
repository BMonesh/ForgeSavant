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
import re
from datetime import UTC, datetime

import pandas as pd

# ── Config ────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLEANED_DIR = os.path.join(BASE_DIR, "cleaned_data")
VERIFIED_IDENTITY_DIR = os.path.join(BASE_DIR, "verified_identity")

MANIFEST_FILES = {
    "processors": "processors.json",
    "gpus": "gpus.json",
    "motherboards": "motherboards.json",
    "ram": "ram.json",
    "storage": "storage.json",
    "power_supplies": "power-supplies.json",
    "cabinets": "cabinets.json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def string_value(value, default="") -> str:
    return default if value is None or pd.isna(value) else str(value)


def transform_provenance(row: pd.Series) -> dict:
    """Attach honest source and freshness metadata to every catalog record."""
    collected_at = row.get("collected_at", "")
    if pd.isna(collected_at):
        collected_at = ""
    return {
        "source": string_value(row.get("source"), "unknown"),
        "source_url": string_value(row.get("source_url")),
        "source_item_id": string_value(row.get("source_item_id")),
        "currency": string_value(row.get("currency"), "INR"),
        "availability": string_value(row.get("availability"), "unknown"),
        "collected_at": str(collected_at),
        "data_status": string_value(row.get("data_status"), "sample"),
    }


def transform_identity(row: pd.Series) -> dict:
    """Preserve verified manufacturer identity fields when supplied."""
    return {
        "manufacturerPartNumber": string_value(row.get("manufacturer_part_number")),
        "manufacturerPartNumberSourceUrl": string_value(row.get("manufacturer_part_number_source_url")),
    }


def normalized_identity_name(value: str) -> str:
    """Normalize display-name punctuation/casing for manifest lookups."""
    return " ".join(re.findall(r"[a-z0-9]+", str(value).lower()))


def canonical_key(component: str, name: str) -> str:
    category = "powerSupplies" if component == "power_supplies" else component
    return f"{category}:{normalized_identity_name(name).replace(' ', '-')}"


def load_verified_manifest(component: str) -> list[dict]:
    manifest_file = MANIFEST_FILES.get(component)
    if not manifest_file:
        raise ValueError(f"No verified identity manifest configured for {component}")
    with open(os.path.join(VERIFIED_IDENTITY_DIR, manifest_file), encoding="utf-8") as handle:
        return json.load(handle)


def apply_verified_identity(component: str, document: dict, manifest: list[dict] | None = None) -> dict:
    """Overlay the curated row with its mandatory manufacturer-verified identity."""
    records = manifest if manifest is not None else load_verified_manifest(component)
    lookup = {}
    for record in records:
        for candidate in (record.get("currentName"), record.get("name")):
            if candidate:
                lookup[normalized_identity_name(candidate)] = record
    original_name = document["name"]
    record = lookup.get(normalized_identity_name(original_name))
    if not record:
        raise ValueError(f"No verified identity for {component} product: {original_name}")

    verified_name = record["name"]
    aliases = list(dict.fromkeys(value for value in (original_name, record.get("currentName")) if value and value != verified_name))
    document["name"] = verified_name
    document["manufacturer"] = record.get("manufacturer", document["manufacturer"])
    document["specifications"] = {**document.get("specifications", {}), **record.get("specifications", {})}
    document["identity"] = {
        "canonicalKey": canonical_key(component, verified_name),
        "manufacturerPartNumber": record["manufacturerPartNumber"],
        "manufacturerPartNumberSourceUrl": record["sourceUrl"],
        "manufacturerPartNumberVerifiedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "aliases": aliases,
        "lifecycleStatus": "unknown",
    }
    return document


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
        "identity": transform_identity(row),
        "provenance": transform_provenance(row),
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
        "identity": transform_identity(row),
        "provenance": transform_provenance(row),
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
            "memory_type": str(row.get("memory_type", "")),
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
        "identity": transform_identity(row),
        "provenance": transform_provenance(row),
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
        "identity": transform_identity(row),
        "provenance": transform_provenance(row),
    }


def transform_storage(row: pd.Series) -> dict:
    return {
        "name": str(row["name"]),
        "type": str(row.get("type", "Storage")),
        "manufacturer": str(row["manufacturer"]),
        "specifications": {
            "capacity": str(row.get("capacity", "")),
            "interface": str(row.get("interface", "")),
            "form_factor": str(row.get("form_factor", "")),
            "speed": str(row.get("speed", "")),
            "technology": str(row.get("technology", "")),
            "encryption": string_value(row.get("encryption", "")),
            "tbw": string_value(row.get("tbw", "")),
            "warranty": str(row.get("warranty", "")),
        },
        "price": float(row["price"]),
        "image_url": string_value(row.get("image_url")),
        "identity": transform_identity(row),
        "provenance": transform_provenance(row),
    }


def transform_power_supply(row: pd.Series) -> dict:
    return {
        "name": str(row["name"]),
        "type": str(row.get("type", "Power Supply")),
        "manufacturer": str(row["manufacturer"]),
        "specifications": {
            "wattage": str(row.get("wattage", "")),
            "efficiency": str(row.get("efficiency", "")),
            "modular": str(row.get("modular", "false")).lower() == "true",
            "certifications": [value.strip() for value in str(row.get("certifications", "")).split("|") if value.strip()],
            "fan_size": str(row.get("fan_size", "")),
            "dimensions": str(row.get("dimensions", "")),
            "weight": str(row.get("weight", "")),
        },
        "price": float(row["price"]),
        "identity": transform_identity(row),
        "provenance": transform_provenance(row),
    }


def transform_cabinet(row: pd.Series) -> dict:
    return {
        "name": str(row["name"]),
        "type": str(row.get("type", "Cabinet")),
        "manufacturer": str(row["manufacturer"]),
        "specifications": {
            "form_factor": str(row.get("form_factor", "")),
            "motherboard_support": str(row.get("motherboard_support", "")),
            "fan_support": str(row.get("fan_support", "")),
            "radiator_support": str(row.get("radiator_support", "")),
            "gpu_clearance": str(row.get("gpu_clearance", "")),
            "cpu_cooler_clearance": str(row.get("cpu_cooler_clearance", "")),
            "storage": str(row.get("storage", "")),
            "dimensions": str(row.get("dimensions", "")),
        },
        "price": float(row["price"]),
        "image_url": string_value(row.get("image_url")),
        "identity": transform_identity(row),
        "provenance": transform_provenance(row),
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
    "motherboards": ["name", "manufacturer", "specifications.socket", "specifications.chipset", "specifications.memory_type", "price"],
    "ram": ["name", "manufacturer", "specifications.capacity", "specifications.type", "price"],
    "storage": ["name", "manufacturer", "specifications.capacity", "specifications.interface", "price"],
    "power_supplies": ["name", "manufacturer", "specifications.wattage", "price"],
    "cabinets": ["name", "manufacturer", "specifications.motherboard_support", "price"],
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
    "storage": {
        "csv": os.path.join(CLEANED_DIR, "storage_cleaned.csv"),
        "collection": "storages",
        "transformer": transform_storage,
    },
    "power_supplies": {
        "csv": os.path.join(CLEANED_DIR, "power_supplies_cleaned.csv"),
        "collection": "powersupplies",
        "transformer": transform_power_supply,
    },
    "cabinets": {
        "csv": os.path.join(CLEANED_DIR, "cabinets_cleaned.csv"),
        "collection": "cabinets",
        "transformer": transform_cabinet,
    },
}


def component_documents(component: str) -> tuple[list[dict], int]:
    """Transform, verify, and collapse duplicate source rows by exact MPN."""
    config = COMPONENT_CONFIG[component]
    df = pd.read_csv(config["csv"])
    manifest = load_verified_manifest(component)
    documents_by_mpn = {}
    for _, row in df.iterrows():
        document = apply_verified_identity(component, config["transformer"](row), manifest)
        mpn = document["identity"]["manufacturerPartNumber"]
        existing = documents_by_mpn.get(mpn)
        # Seed data is a planning baseline. For duplicate source rows retain the
        # lowest planning price deterministically; live offers use the signed feed path.
        if existing is None or document["price"] < existing["price"]:
            documents_by_mpn[mpn] = document
    return list(documents_by_mpn.values()), len(df)


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

    try:
        documents, input_rows = component_documents(component)
    except Exception as error:
        return {"component": component, "collection": config["collection"], "error": str(error)}
    results = {
        "component": component,
        "collection": config["collection"],
        "input_rows": input_rows,
        "total_rows": len(documents),
        "duplicates_collapsed": input_rows - len(documents),
        "valid": 0,
        "invalid": 0,
        "validation_errors": [],
        "sample_documents": [],
    }

    for idx, doc in enumerate(documents):
        try:
            errors = validate_document(doc, required)
            if errors:
                results["invalid"] += 1
                results["validation_errors"].append({
                    "row": idx,
                    "name": doc.get("name", "unknown"),
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

    try:
        documents, input_rows = component_documents(component)
    except Exception as error:
        return {"component": component, "collection": collection_name, "error": str(error)}
    client = MongoClient(mongo_uri)
    db = client.get_default_database()
    collection = db[collection_name]

    results = {
        "component": component,
        "collection": collection_name,
        "input_rows": input_rows,
        "duplicates_collapsed": input_rows - len(documents),
        "inserted": 0,
        "updated": 0,
        "errors": 0,
        "error_details": [],
    }

    for doc in documents:
        try:
            identity = doc["identity"]
            provenance = doc["provenance"]
            if provenance.get("data_status") not in {"sample", "seed", "fixture"}:
                raise ValueError("Direct importer accepts seed/sample data only; use the signed partner-feed path for live offers")
            aliases = identity.get("aliases", [])
            match_names = list(dict.fromkeys([doc["name"], *aliases]))
            update_fields = {
                key: value for key, value in doc.items()
                if key not in {"price", "provenance", "priceHistory", "identity"}
            }
            update_fields.update({f"identity.{key}": value for key, value in identity.items() if key != "aliases"})
            initial_history = [{
                "price": doc["price"],
                "currency": provenance.get("currency", "INR"),
                "availability": provenance.get("availability", "unknown"),
                "source": provenance.get("source", "catalog seed"),
                "sourceUrl": provenance.get("source_url", ""),
                "sourceItemId": provenance.get("source_item_id", ""),
                "observedAt": provenance.get("collected_at") or None,
                "recordedAt": datetime.now(UTC),
            }]
            result = collection.update_one(
                {"$or": [
                    {"identity.manufacturerPartNumber": identity["manufacturerPartNumber"]},
                    {"name": {"$in": match_names}},
                    {"identity.aliases": {"$in": match_names}},
                ]},
                {
                    "$set": update_fields,
                    "$addToSet": {"identity.aliases": {"$each": aliases}},
                    "$setOnInsert": {"price": doc["price"], "provenance": provenance, "priceHistory": initial_history},
                },
                upsert=True,
            )
            if result.upserted_id:
                results["inserted"] += 1
            else:
                results["updated"] += 1
        except Exception as e:
            results["errors"] += 1
            results["error_details"].append({
                "name": doc.get("name", "unknown"),
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
            if result.get("error"):
                print(f"  ERROR: {result['error']}")
                raise SystemExit(1)
            print(f"  Input rows:        {result.get('input_rows', 0)}")
            print(f"  Total rows:        {result.get('total_rows', 0)}")
            print(f"  Duplicates merged: {result.get('duplicates_collapsed', 0)}")
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
