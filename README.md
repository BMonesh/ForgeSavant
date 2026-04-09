# ForgeSavant: Crafting Savvy PC Builds

ForgeSavant is a full-stack web application that helps users build custom PC configurations with real-time compatibility checking, performance benchmarking, and price comparison. It combines an intuitive React-based builder interface with a Python data pipeline that scrapes, cleans, and normalizes hardware specification data from multiple vendor sources.

## Features

- **Virtual PC Builder**: Create and customize PC configurations using a drag-and-drop interface with real-time feedback.
- **Compatibility Checker**: Rule-based engine that validates component compatibility across 50+ constraint parameters (CPU socket, RAM type, power budget, form factor).
- **Performance Assessment**: Benchmark scores and comparisons to evaluate build performance before purchasing.
- **Data Pipeline**: Python-based ETL pipeline that scrapes hardware data from multiple Indian vendors, cleans inconsistent formats, deduplicates entries, and imports normalized data into MongoDB.
- **User Profiles**: Save and manage multiple PC configurations under personalized profiles with Google OAuth.

## Tech Stack

| Layer | Technologies |
|---|---|
| **Frontend** | React.js, CSS3, Vite |
| **Backend** | Node.js, Express.js, REST APIs |
| **Database** | MongoDB, Mongoose ODM |
| **Data Pipeline** | Python, Pandas, NumPy, BeautifulSoup |
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

# Clean raw data from multiple vendor sources
python data_cleaner.py --all --stats

# Run compatibility validation on sample builds
python compatibility_engine.py --demo

# Preview MongoDB import without writing (dry run)
python import_to_mongo.py --dry-run --all
```

**What it handles:**
- Normalizes inconsistent formats across vendors (`3.7 ghz` -> `3.7 GHz`, `amd` -> `AMD`, `LGA1700` -> `LGA 1700`)
- Deduplicates entries from amazon.in, flipkart.com, and mdcomputers.in (keeps lowest price)
- Transforms flat CSV rows into nested MongoDB documents matching the Mongoose schemas
- Validates hardware compatibility (CPU-motherboard socket, RAM-DDR type, power budget)

See [`data-pipeline/README.md`](data-pipeline/README.md) for detailed usage.

## Getting Started

### Prerequisites
- Node.js (v16+)
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

## Deployed Links

- **Frontend**: [Live App](https://66afbd0b1a567edb42e38508--stellular-pony-37d1ec.netlify.app/)
- **Backend API**: [API Server](https://s51-monesh-capstone-forgesavant.onrender.com)

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
