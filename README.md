# ForgeSavant: Crafting Savvy PC Builds

ForgeSavant is a full-stack web application for planning custom PC configurations with evidence-backed compatibility checks and India-first catalog pricing. It combines a guided React builder with an Express API, MongoDB catalog, and Python pipeline that cleans, validates, and imports hardware data.

## Features

- **Guided PC Builder**: Build a configuration through a nine-step component workflow with searchable, filterable catalog choices.
- **Compatibility Checker**: Server-side rules evaluate CPU socket, memory generation, power headroom, motherboard/case fit, and storage interfaces.
- **Transparent Estimates**: Price and power values are labeled as planning data unless an authorized feed supplies a traceable observation.
- **Catalog Evidence**: Canonical product identities, aliases, price history, retailer mappings, and source freshness are inspectable from the builder.
- **Reviewed Product Content**: Open Icecat specifications and media land in an immutable lake, pass exact-identity review, and are promoted as evidence without overwriting compatibility rules.
- **Integrated Data Pipeline**: Python ETL validates and imports processors, GPUs, motherboards, RAM, storage, power supplies, and cabinets with provenance metadata.
- **Data Health Console**: Administrators can inspect catalog coverage, validation, freshness, source limitations, and pipeline execution health.
- **User Profiles**: Save compatible builds and manage them through password or verified Google sign-in.

## Tech Stack

| Layer | Technologies |
|---|---|
| **Frontend** | React.js, CSS3, Vite |
| **Backend** | Node.js, Express.js, REST APIs |
| **Database** | MongoDB, Mongoose ODM |
| **Data Pipeline** | Python ETL and validation scripts |
| **Auth** | Google OAuth, JWT, bcrypt |
| **Deployment** | Netlify (frontend), Render (backend) |

## Project Structure

```
ForgeSavant/
├── client/frontEnd/        # React frontend (Vite)
│   └── src/
│       ├── Components/     # Build, Login, Signup, Profile, Navbar
│       └── Styles/         # Component-specific CSS
├── models/                 # Mongoose schemas
│   ├── processor.model.js
│   ├── graphicsCard.model.js
│   ├── motherboard.model.js
│   ├── ram.model.js
│   ├── storage.model.js
│   ├── smps.model.js
│   └── cabinet.model.js
├── routes/                 # Express API routes
├── data-pipeline/          # Python data processing pipeline
│   ├── raw_data/           # Scraped CSVs from vendor sources
│   ├── cleaned_data/       # Normalized, deduplicated CSVs
│   ├── scraper.py          # Web scraper with rate limiting
│   ├── data_cleaner.py     # Pandas-based cleaning & normalization
│   ├── compatibility_engine.py  # Rule-based hardware validation
│   └── import_to_mongo.py  # CSV -> MongoDB document importer
├── server.js               # Express server entry point
└── package.json
```

## Data Pipeline

The `data-pipeline/` directory contains Python scripts for collecting and processing hardware component data:

```bash
cd data-pipeline
pip install -r requirements.txt

# Clean available raw component data
python data_cleaner.py --all --stats

# Run compatibility validation on sample builds
python compatibility_engine.py --demo

# Preview MongoDB import without writing (dry run)
python import_to_mongo.py --dry-run --all
```

**What it handles:**
- Normalizes inconsistent formats across vendors (`3.7 ghz` -> `3.7 GHz`, `amd` -> `AMD`, `LGA1700` -> `LGA 1700`)
- Deduplicates entries from configured sources (keeps the lowest price)
- Transforms flat CSV rows into nested MongoDB documents matching the Mongoose schemas
- Validates hardware compatibility (CPU-motherboard socket, RAM-DDR type, power budget)

See [`data-pipeline/README.md`](data-pipeline/README.md) for detailed usage.

## Getting Started

### Prerequisites
- Node.js 20.19+ (or 22.12+)
- MongoDB (local or Atlas)
- Python 3.10+ (for data pipeline)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/BMonesh/ForgeSavant.git
   cd ForgeSavant
   ```

2. Install backend dependencies:
   ```bash
   npm install
   ```

3. Set up environment variables:
   ```bash
   cp .env.example .env
   # Add your MongoDB URI, JWT_SECRET, and Google OAuth credentials
   ```

4. Install frontend dependencies:
   ```bash
   cd client/frontEnd
   npm install
   ```

5. Start the development server:
   ```bash
   # From root directory
   npm start
   ```

6. (Optional) Set up the data pipeline:
   ```bash
   cd data-pipeline
   pip install -r requirements.txt
   python data_cleaner.py --all --stats
   ```

7. (Optional) enable the Flipkart connector only after an affiliate account is
   approved and the API is available. Add
   `FLIPKART_AFFILIATE_ID` and `FLIPKART_AFFILIATE_TOKEN` to your local
   environment. From the repository root:

   ```bash
   npm run pipeline:sync:dry  # inspect match report; does not change CSVs
   npm run pipeline:sync      # export verified matches for signed admin review
   ```

   Upload the generated ignored `data-pipeline/authorized_offer_feed.json` in
   the admin Data import screen; the connector never rewrites seed CSVs or the
   database directly. If affiliate registration is unavailable, use the authorized partner-feed
   upload described below; the application remains fully usable with clearly
   labeled sample pricing. Retail offers are matched against curated identities and only overlay
   price/image/provenance. The API exposes each value as live, stale, or sample;
   `CATALOG_FRESHNESS_HOURS` controls the live-price window (default: 24).

### Authorized partner feeds

If a retailer supplies a CSV or JSON file, add the operator email to
`ADMIN_EMAILS`, restart the API, and sign in again. Admin users receive a
**Data import** navigation item where they can download the feed template,
preview model matches, inspect rejected/ambiguous rows, and apply only verified
offers. The server signs previews for 15 minutes and records every applied batch
by checksum and operator. Ambiguous rows can be manually resolved; the approved
retailer-product mapping is reused on later feeds. No imported feed can modify
compatibility specs.

Catalog maintenance commands:

```bash
npm run catalog:audit    # preview identity enrichment and safe duplicate merges
npm run catalog:migrate  # back up, enrich, and apply safe merges
npm run catalog:quality  # profile identity, provenance, mappings, and saved references
npm run catalog:identity:audit  # validate manufacturer identity evidence without writes
npm run catalog:identity:apply  # back up and apply conflict-free identity evidence
npm run pipeline:run    # ingest Open Icecat observations and rebuild analytics safely
npm run catalog:content:export   # build the ignored admin-review feed from the lake
npm run catalog:content:preview  # exact-MPN Atlas match; never writes
npm run catalog:content:apply    # append the reviewed evidence batch idempotently
npm run analytics:notebook  # execute the reproducible ML-readiness assessment
npm run verify           # full backend, pipeline, catalog, frontend, lint, and build gate
```

### Production configuration

Production startup fails closed unless `URI`, a unique 32+ character
`JWT_SECRET`, and HTTPS-only `ALLOWED_ORIGINS` are explicitly configured. Set
`NODE_ENV=production`; configure `GOOGLE_CLIENT_ID` only if Google sign-in is
enabled. Never commit the resulting `.env` file.

See [`CATALOG_DATA_QUALITY.md`](CATALOG_DATA_QUALITY.md) for the current measured baseline.

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/CPU` | List all processors |
| GET | `/GPU` | List all graphics cards |
| GET | `/motherboard` | List all motherboards |
| GET | `/ram` | List all RAM modules |
| GET | `/storage` | List all storage devices |
| GET | `/smps` | List all power supplies |
| GET | `/cabinet` | List all cabinets |
| POST | `/login` | User authentication |
| POST | `/signup` | User registration |
| POST | `/saves` | Save a PC build configuration |
| GET | `/api/v1/catalog` | Fetch the complete seven-category catalog and metadata |
| GET | `/api/v1/catalog/:category/:id` | Inspect canonical identity, price history, provenance, and retailer mappings |
| POST | `/api/v1/compatibility/evaluate` | Evaluate a build using database component IDs |
| POST | `/api/v1/analytics/estimate` | Return a versioned, low-confidence planning estimate for a CPU/GPU pair |
| GET | `/api/v1/admin/offers/status` | Verify administrator offer-import access |
| GET | `/api/v1/admin/offers/history` | List recent applied partner-feed batches |
| POST | `/api/v1/admin/offers/preview` | Validate and match an authorized CSV/JSON feed payload |
| POST | `/api/v1/admin/offers/apply` | Apply accepted rows from a signed, unexpired preview |
| GET | `/api/v1/admin/content/history` | List reviewed product-content import batches |
| POST | `/api/v1/admin/content/preview` | Validate and exactly match an Open Icecat evidence feed |
| POST | `/api/v1/admin/content/apply` | Append evidence from a signed, unexpired review |
| GET | `/api/v1/admin/analytics/data-quality` | Return the protected data-quality and pipeline-health summary |
| GET | `/health` | Application and database health |
| GET | `/ready` | Readiness status |
| GET | `/live` | Process liveness status |

## Deployment

The repository does not claim a production URL until a deployment passes
`npm run verify`, uses production secrets, and connects to an authorized live
pricing source. The frontend and API can be deployed separately; set the
frontend API base URL and server `ALLOWED_ORIGINS` to their final HTTPS URLs.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m "Add your feature"`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Open a Pull Request

## License

Licensed under the [MIT License](LICENSE).

## Contact

- **Email**: [2005.monesh@gmail.com](mailto:2005.monesh@gmail.com)
- **LinkedIn**: [Monesh B](https://www.linkedin.com/in/monesh-b-053439289/)
