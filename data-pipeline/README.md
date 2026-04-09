# ForgeSavant Data Pipeline

Python-based data processing pipeline that collects, cleans, validates, and imports hardware component data into the ForgeSavant MongoDB database.

## Pipeline Overview

```
raw_data/ (scraped CSVs)
    │
    ├── scraper.py          → Collects data from vendor sources
    │
    ├── data_cleaner.py     → Cleans, normalizes, deduplicates
    │
    ├── cleaned_data/       → Analysis-ready CSVs
    │
    ├── compatibility_engine.py  → Rule-based hardware validation
    │
    └── import_to_mongo.py  → Imports to MongoDB (matches Mongoose schemas)
```

## Setup

```bash
cd data-pipeline
pip install -r requirements.txt
```

## Usage

### 1. Clean Raw Data
```bash
# Clean all component types
python data_cleaner.py --all --stats

# Clean specific component
python data_cleaner.py --component processors --stats
```

Handles: inconsistent casing, duplicate entries across vendors, unit normalization (GHz/MHz, wattage, voltage), missing values.

### 2. Validate Compatibility
```bash
# Run demo with sample builds
python compatibility_engine.py --demo

# Find compatible motherboards for a CPU
python compatibility_engine.py --find-compatible --cpu "AMD Ryzen 5 5600X"

# Validate a custom build from JSON
python compatibility_engine.py --check-build my_build.json
```

### 3. Import to MongoDB
```bash
# Dry run (validate without importing)
python import_to_mongo.py --dry-run --all

# Import to database
python import_to_mongo.py --all --uri mongodb://localhost:27017/forgesavant
```

### 4. Generate Scrape Report
```bash
python scraper.py --report
```

## Data Schema

The pipeline transforms flat CSV data into nested MongoDB documents matching the Mongoose schemas in `/models`:

| CSV Column | MongoDB Path |
|---|---|
| `cores` | `specifications.cores` |
| `base_clock` | `specifications.base_clock` |
| `socket` | `specifications.socket` |
| ... | `specifications.*` |

## Cleaning Rules

- **Manufacturers**: Canonical casing (`amd` → `AMD`, `nvidia` → `NVIDIA`)
- **Clock speeds**: `3.7 ghz` → `3.7 GHz`, `2460 mhz` → `2460 MHz`
- **Sockets**: `lga 1700` / `LGA1700` → `LGA 1700`
- **TDP**: `65 W` / `65w` → `65W`
- **Deduplication**: Same component from multiple vendors → keeps lowest price
