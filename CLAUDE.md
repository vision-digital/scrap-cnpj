# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a complete data pipeline for downloading, processing, and exposing Brazilian public CNPJ (business registry) data. The architecture consists of three main components:

- **Backend (FastAPI/Python)**: Orchestrates download, extraction, loading (using PostgreSQL COPY), and provides search/export APIs
- **PostgreSQL 16**: Stores millions of records with BTREE and GIN (pg_trgm) indexes for fast queries
- **Frontend (React + Vite)**: Interface for filters, CSV exports, and version tracking

## Development Setup

### Prerequisites
Create `.env` from `.env.example` with PostgreSQL credentials.

### Running the Application

```bash
# Start all services (Postgres, Backend, Frontend)
docker compose up --build

# Frontend: http://localhost:5173
# Backend API: http://localhost:8000
# PostgreSQL: localhost:5432
```

### Running Backend Locally (without Docker)

```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Running Frontend Locally (without Docker)

```bash
cd frontend
npm install
npm run dev  # Development server
npm run build  # Production build
npm run preview  # Preview production build
```

### Manual Data Updates

```bash
cd backend
python -m app.tasks.update_data  # Uses latest release
python -m app.tasks.update_data --release 2025-11  # Specific release
```

## Architecture

### Backend Structure

```
backend/app/
├── api/routes/       - API endpoint definitions (health, search, export, stats, update, version)
├── core/            - Configuration (Settings via pydantic-settings with SCRAP_ prefix)
├── db/              - Database utilities (postgres.py, schema.py, session.py, utils.py)
├── models/          - SQLAlchemy ORM models (entities.py contains Empresa, Estabelecimento, Socio, Simples, DataVersion)
├── schemas/         - Pydantic schemas for API request/response validation
├── services/        - Business logic layer
│   ├── downloader.py      - Downloads ZIP files from Receita Federal
│   ├── extractor.py       - Extracts ZIPs to staging
│   ├── loader.py          - Loads CSV data using COPY FROM STDIN (50k batch size)
│   ├── pipeline.py        - Orchestrates download → extract → load
│   ├── receita_client.py  - HTTP client for Receita Federal website
│   └── versioning.py      - Tracks data versions in data_versions table
├── tasks/           - CLI tasks (update_data.py using Typer)
└── main.py          - FastAPI app initialization
```

### Database Schema

The loader service (`services/loader.py`) processes four types of files identified by their signatures:

- **EMPRECSV** → `empresas` table: Base CNPJ data (cnpj_basico as PK, razao_social, natureza_juridica, capital_social, porte_empresa)
- **ESTABELE** → `estabelecimentos` table: Establishment data (cnpj14 as PK, nome_fantasia, UF, municipio, CNAE, address details)
- **SOCIOCSV** → `socios` table: Partner/shareholder data (synthetic ID PK, cnpj_basico indexed, nome_socio, cpf_cnpj_socio)
- **SIMECSV** → `simples` table: Simples Nacional and MEI status (cnpj_basico as PK, opcao_simples, opcao_mei)

All tables use GIN trigram indexes (pg_trgm) on text fields for fuzzy search capabilities. The schema is created automatically on startup via `db/schema.py:ensure_tables()`.

### Pipeline Flow

The data update pipeline (`services/pipeline.py`) executes in this order:

1. **Discovery**: ReceitaFederalClient reads official index to find latest release folder
2. **Download**: DownloadManager fetches all ZIPs to `data/raw/<release>` (reused if present)
3. **Extract**: Extractor unpacks files to `data/staging/<release>` (reused if present)
4. **Load**: Loader processes each CSV using `COPY FROM STDIN` in batches, with tqdm progress bars
5. **Versioning**: DataVersion table tracks release, status (running/completed/failed), timestamps
6. **Cleanup**: Optional removal of raw/staging files based on `SCRAP_CLEANUP_*` settings

The loader (`services/loader.py`) uses custom builder functions for each dataset type to clean and pad CNPJ numbers, handle capital_social decimal formatting, and construct the 14-digit CNPJ for establishments.

### Configuration

All settings are centralized in `core/config.py` using pydantic-settings with `SCRAP_` prefix:

- `SCRAP_PG_*` - PostgreSQL connection details
- `SCRAP_DATA_DIR` - Root directory for downloads/extractions (default: /data)
- `SCRAP_REUSE_DOWNLOADS` - Skip redownloading existing files (default: true)
- `SCRAP_REUSE_EXTRACTIONS` - Skip re-extracting existing files (default: true)
- `SCRAP_CLEANUP_RAW_AFTER_LOAD` - Remove ZIP files after successful load (default: false)
- `SCRAP_CLEANUP_STAGING_AFTER_LOAD` - Remove extracted CSVs after load (default: false)

### Key APIs

- `GET /api/health` - Health check
- `GET /api/version/latest` - Current data version
- `GET /api/updates/status` - Pipeline execution status
- `POST /api/updates/run` - Trigger data update pipeline
- `GET /api/search/<dataset>` - Search empresas/estabelecimentos/socios/simples with filters
- `GET /api/export/<dataset>` - Export filtered results as CSV
- `GET /api/stats` - Record counts and current release

### Loader Implementation Details

The loader (`services/loader.py`) uses a batching approach for efficient data insertion:

1. Reads CSV files with `;` delimiter from Receita Federal
2. Processes rows through dataset-specific builder functions that clean/pad CNPJ numbers and format values
3. Writes data to PostgreSQL using `COPY FROM STDIN` with internally-generated CSV format
4. Uses `io.StringIO` buffer with Python's `csv.writer` to properly escape and format values
5. Processes in batches of 5000 rows for optimal memory usage
6. Commits only after entire file is processed to ensure atomicity

Performance: ~150-170k rows/second on typical hardware.

### Frontend Structure

```
frontend/src/
├── api/         - API client (client.ts with axios)
├── components/  - React components (currently empty - components defined in App.tsx)
├── App.tsx      - Main application component
├── main.tsx     - React entry point
└── styles.css   - Global styles
```

The frontend is a TypeScript React application using Vite. API base URL is configured via `VITE_API_URL` environment variable.

## Common Development Tasks

### Adding a New API Endpoint

1. Create route file in `backend/app/api/routes/` (e.g., `myroute.py`)
2. Define FastAPI router with endpoints
3. Register in `backend/app/main.py` via `app.include_router(myroute.router, prefix=settings.api_prefix)`

### Modifying Database Schema

1. Update SQLAlchemy models in `backend/app/models/entities.py`
2. Add/modify indexes in `backend/app/db/schema.py:_ensure_indexes()`
3. Schema changes apply automatically on container restart (tables created via `Base.metadata.create_all()`)

### Adding New Dataset Type

1. Add dataset configuration to `DATASETS` dict in `backend/app/services/loader.py`
2. Define signature (filename pattern), table name, columns, and builder function
3. Builder function must clean/transform raw CSV row into proper format

### Monitoring and Debugging

```bash
# View backend logs with pipeline progress
docker compose logs backend -f

# View PostgreSQL logs
docker compose logs postgres -f

# Check current data statistics
curl http://localhost:8000/api/stats

# Check pipeline status
curl http://localhost:8000/api/updates/status
```

The loader uses tqdm to show real-time progress for each file being loaded. Logs indicate start/end of each pipeline stage with row counts.

## Data Directory Structure

```
data/
├── raw/<release>/        - Downloaded ZIP files
└── staging/<release>/    - Extracted CSV files
```

Files are reused across runs by default to save bandwidth and processing time. Configure cleanup behavior via environment variables.
