# 🌾 Agri-Climate Data Pipeline

A **zero-cost, fully local** ELT data pipeline that ingests agricultural crop-yield data and historical climate data for **East Africa** (Kenya, Rwanda, Uganda, Tanzania), transforms it through a Medallion Architecture (Bronze → Silver → Gold), and produces an analytics-ready **Climate Impact Analysis** dataset — all orchestrated with **Kestra**.

---

## 📑 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Running the Pipeline](#running-the-pipeline)
- [Pipeline Stages](#pipeline-stages)
  - [1. Source System — PostgreSQL](#1-source-system--postgresql)
  - [2. Bronze Layer — Raw Ingestion](#2-bronze-layer--raw-ingestion)
  - [3. Silver Layer — Cleaning & Aggregation](#3-silver-layer--cleaning--aggregation)
  - [4. Gold Layer — Business Analytics](#4-gold-layer--business-analytics)
- [dbt Project — agri_climate_models](#dbt-project--agri_climate_models)
  - [Staging Models](#staging-models)
  - [Mart Models](#mart-models)
  - [Seeds](#seeds)
  - [Macros](#macros)
  - [Tests](#tests)
- [Infrastructure (Terraform)](#infrastructure-terraform)
- [Orchestration (Kestra)](#orchestration-kestra)
- [Services & Ports](#services--ports)
- [Data Sources](#data-sources)
- [Environment Variables](#environment-variables)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

This pipeline answers a core question: **How does climate variability (rainfall, temperature) impact crop yields across East African regions?**

It does so by:

1. **Seeding** a PostgreSQL database with real FAOSTAT/Kaggle crop-yield data for Kenya, Rwanda, Uganda, and Tanzania.
2. **Extracting** that relational data and historical weather data (from the [Open-Meteo API](https://open-meteo.com/)) into a local **MinIO** data lake as Parquet files (Bronze layer).
3. **Transforming** the data through Silver (cleaned/aggregated) and Gold (joined analytical) layers using **DuckDB** for raw SQL transforms and **dbt** for modular, tested, version-controlled transformations.
4. **Orchestrating** the entire flow end-to-end with **Kestra**.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          ORCHESTRATION (Kestra)                        │
│   bronze_ingestion_flow.yml — automates every step below sequentially  │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
 ┌──────────────┐   ┌───────────────┐   ┌────────────────┐
 │  seed_db.py  │   │  Open-Meteo   │   │    Kaggle /    │
 │  (Postgres)  │   │     API       │   │   FAOSTAT CSV  │
 └──────┬───────┘   └───────┬───────┘   └────────────────┘
        │                   │
        ▼                   ▼
 ┌────────────────────────────────────────────┐
 │         MinIO Data Lake (S3-compatible)    │
 │                                            │
 │  bronze/                                   │
 │  ├── crop_data/                            │
 │  │   ├── regions.parquet                   │
 │  │   ├── fields.parquet                    │
 │  │   └── harvest_yields.parquet            │
 │  └── climate_data/                         │
 │      └── historical_weather.parquet        │
 │                                            │
 │  silver/                                   │
 │  ├── denormalized_crops.parquet            │
 │  └── yearly_weather.parquet                │
 │                                            │
 │  gold/                                     │
 │  └── climate_impact_analysis.parquet       │
 └────────────────────────────────────────────┘
        │
        ▼
 ┌────────────────────────────────────────────┐
 │         dbt + DuckDB Warehouse             │
 │  staging (views)  →  marts (tables)        │
 │  stg_crops, stg_weather →                  │
 │       climate_impact_analysis              │
 └────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer            | Technology                                                                                  |
| ---------------- | ------------------------------------------------------------------------------------------- |
| **Source DB**     | PostgreSQL 15 (Dockerized)                                                                  |
| **Data Lake**    | MinIO (S3-compatible object storage, Dockerized)                                            |
| **Transforms**   | DuckDB (in-process OLAP engine) + dbt-duckdb                                                |
| **Orchestration**| Kestra (workflow orchestrator, Dockerized)                                                   |
| **DB Admin UI**  | pgAdmin 4 (Dockerized)                                                                      |
| **Data Format**  | Apache Parquet (via PyArrow)                                                                 |
| **Language**     | Python 3.14 (managed with `uv`)                                                             |
| **IaC**          | Terraform (Google Cloud — GCS bucket + BigQuery dataset for optional cloud deployment)       |

---

## Project Structure

```
agri-climate-pipeline/
│
├── .env                          # Environment variables (Postgres credentials)
├── .gitignore                    # Ignores keys, .env, CSVs, Terraform state
├── .python-version               # Python version (3.14)
├── pyproject.toml                # Project metadata & dependencies (uv-managed)
├── uv.lock                      # Lockfile for deterministic installs
├── README.md                     # ← You are here
│
├── docker/
│   ├── docker-compose.yml        # Spins up Postgres, pgAdmin, MinIO, Kestra
│   └── logs/
│       └── dbt.log               # Docker-context dbt logs
│
├── infrastructure/
│   ├── main.tf                   # Terraform: GCS bucket + BigQuery dataset
│   ├── variables.tf              # Terraform: project_id, region
│   ├── .terraform.lock.hcl       # Provider lock file
│   └── keys/                     # GCP service account key (git-ignored)
│
├── ingestion/
│   ├── seed_db.py                # Seeds Postgres with FAOSTAT crop-yield data
│   ├── extract_postgres_to_minio.py  # Extracts Postgres tables → MinIO Parquet
│   ├── extract_weather_to_minio.py   # Fetches Open-Meteo API → MinIO Parquet
│   ├── transform_duckdb.py       # DuckDB SQL transforms (Bronze → Silver → Gold)
│   ├── yield.csv                 # FAOSTAT crop yield dataset (git-ignored)
│   ├── yield_df.csv              # Processed yield data
│   ├── pesticides.csv            # Pesticide usage dataset
│   ├── rainfall.csv              # Rainfall dataset
│   └── temp.csv                  # Temperature dataset
│
├── orchestration/
│   └── bronze_ingestion_flow.yml # Kestra flow: full ELT pipeline definition
│
├── warehouse/
│   └── agri_climate_models/      # dbt project root
│       ├── dbt_project.yml       # dbt project configuration
│       ├── profiles.yml          # dbt profile (DuckDB + MinIO S3 connection)
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml   # External source definitions (MinIO Parquet)
│       │   │   ├── schema.yml    # Column-level tests & documentation
│       │   │   ├── stg_crops.sql # Denormalized crop yields view
│       │   │   └── stg_weather.sql   # Aggregated annual weather view
│       │   └── marts/
│       │       └── climate_impact_analysis.sql  # Final analytics table
│       ├── seeds/
│       │   └── crop_categories.csv   # Static lookup: crop → category mapping
│       ├── macros/
│       │   └── convert_kg_to_tons.sql  # Reusable Jinja macro
│       ├── tests/
│       │   └── assert_positive_metrics.sql  # Singular test: no negative values
│       ├── snapshots/            # SCD tracking (placeholder)
│       ├── analyses/             # Ad-hoc analyses (placeholder)
│       └── target/               # dbt build artifacts (auto-generated)
│
└── logs/
    └── dbt.log                   # Local dbt execution logs
```

---

## Getting Started

### Prerequisites

| Tool       | Version  | Purpose                           |
| ---------- | -------- | --------------------------------- |
| Docker     | 20+      | Containerized services            |
| Docker Compose | v2+  | Multi-container orchestration     |
| Python     | 3.10+    | Ingestion & transform scripts     |
| uv         | latest   | Fast Python package manager       |
| Terraform  | 1.5+     | *(Optional)* Cloud infrastructure |

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/NATO-dotcom/Agiclimate-data-pipeline.git
   cd Agiclimate-data-pipeline
   ```

2. **Install Python dependencies**
   ```bash
   uv sync
   ```

3. **Configure environment variables**

   Create a `.env` file in the project root (or use the existing one):
   ```env
   POSTGRES_USER=farm_admin
   POSTGRES_PASSWORD=farm_password
   POSTGRES_DB=farm_management
   ```

4. **Start all services**
   ```bash
   cd docker
   docker compose up -d
   ```
   This launches:
   - **PostgreSQL** on port `5434`
   - **pgAdmin** on port `8080`
   - **MinIO** on ports `9000` (API) / `9001` (Console)
   - **Kestra** on port `8082`

5. **Place the dataset**

   Download the FAOSTAT/Kaggle crop yield CSV and place it at `ingestion/yield.csv`.

### Running the Pipeline

#### Option A — Via Kestra (Recommended)

1. Open the Kestra UI at `http://localhost:8082`
2. Import the flow from `orchestration/bronze_ingestion_flow.yml`
3. Execute the flow — it will:
   - Seed Postgres with FAOSTAT data
   - Extract crop data and weather data to MinIO (Bronze)
   - Run DuckDB transforms (Silver → Gold)
   - Run dbt build (staging views → mart tables)

#### Option B — Manual Execution

```bash
# 1. Seed the source database
uv run python ingestion/seed_db.py

# 2. Extract to MinIO (Bronze layer)
uv run python ingestion/extract_postgres_to_minio.py
uv run python ingestion/extract_weather_to_minio.py

# 3. Transform with DuckDB (Silver + Gold layers)
uv run python ingestion/transform_duckdb.py

# 4. Run dbt transformations
cd warehouse/agri_climate_models
dbt build
```

---

## Pipeline Stages

### 1. Source System — PostgreSQL

[`seed_db.py`](ingestion/seed_db.py) reads the raw FAOSTAT CSV and normalizes it into three relational tables:

| Table              | Description                                              |
| ------------------ | -------------------------------------------------------- |
| `regions`          | East African countries with lat/lon and climate zones    |
| `fields`           | Crop-specific production fields linked to regions        |
| `harvest_yields`   | Yearly yield records (kg) per field and crop type        |

**Target countries**: Kenya, Rwanda, Uganda, Tanzania

### 2. Bronze Layer — Raw Ingestion

| Script                                                          | What it does                                                       |
| --------------------------------------------------------------- | ------------------------------------------------------------------ |
| [`extract_postgres_to_minio.py`](ingestion/extract_postgres_to_minio.py) | Reads all 3 Postgres tables → converts to Parquet → uploads to `s3://agri-data-lake/bronze/crop_data/` |
| [`extract_weather_to_minio.py`](ingestion/extract_weather_to_minio.py)   | Queries the Open-Meteo Historical API (2010–2020) for daily temp & rainfall per region → uploads to `s3://agri-data-lake/bronze/climate_data/` |

### 3. Silver Layer — Cleaning & Aggregation

[`transform_duckdb.py`](ingestion/transform_duckdb.py) connects DuckDB directly to MinIO and performs:

- **Yearly weather aggregation**: Daily weather → annual totals/averages per region
- **Crop denormalization**: Joins `harvest_yields` ← `fields` ← `regions` into one wide table

Output: `s3://agri-data-lake/silver/`

### 4. Gold Layer — Business Analytics

The same DuckDB script joins Silver crops + Silver weather into the final **Climate Impact Analysis** dataset:

| Column              | Description                        |
| ------------------- | ---------------------------------- |
| `region_name`       | Country name                       |
| `crop_type`         | Crop variety                       |
| `harvest_year`      | Year of harvest                    |
| `yield_tons`        | Crop yield in metric tons          |
| `total_rainfall_mm` | Total annual rainfall (mm)         |
| `avg_max_temp`      | Average max temperature (°C)       |

Output: `s3://agri-data-lake/gold/climate_impact_analysis.parquet`

---

## dbt Project — `agri_climate_models`

The dbt layer re-implements the Silver → Gold transformations with full software engineering practices: version control, testing, documentation, and modularity.

### Staging Models

| Model | Materialization | Description |
| ----- | --------------- | ----------- |
| [`stg_crops`](warehouse/agri_climate_models/models/staging/stg_crops.sql) | `view` | Denormalizes yields + fields + regions from Bronze Parquet sources |
| [`stg_weather`](warehouse/agri_climate_models/models/staging/stg_weather.sql) | `view` | Aggregates daily weather into yearly metrics per region |

### Mart Models

| Model | Materialization | Description |
| ----- | --------------- | ----------- |
| [`climate_impact_analysis`](warehouse/agri_climate_models/models/marts/climate_impact_analysis.sql) | `table` | Joins staged crops + weather + seed categories into the final analytics dataset |

### Seeds

| File | Purpose |
| ---- | ------- |
| [`crop_categories.csv`](warehouse/agri_climate_models/seeds/crop_categories.csv) | Static lookup table mapping crop types to categories (Cereal, Legume, Tuber) |

### Macros

| Macro | Purpose |
| ----- | ------- |
| [`convert_kg_to_tons`](warehouse/agri_climate_models/macros/convert_kg_to_tons.sql) | Reusable Jinja macro: divides a column value by 1000 for unit conversion |

### Tests

| Type | Test | Description |
| ---- | ---- | ----------- |
| **Schema** | `not_null` on `region_id`, `harvest_year`, `total_rainfall_mm` | Ensures key columns are never null |
| **Singular** | [`assert_positive_metrics`](warehouse/agri_climate_models/tests/assert_positive_metrics.sql) | Validates that yield and rainfall values are never negative |

---

## Infrastructure (Terraform)

The [`infrastructure/`](infrastructure/) directory provisions **optional** cloud resources on Google Cloud Platform:

| Resource | Name | Purpose |
| -------- | ---- | ------- |
| **GCS Bucket** | `{project_id}-agri-lake` | Cloud data lake (Bronze layer) with 30-day lifecycle cleanup |
| **BigQuery Dataset** | `agri_climate_warehouse` | Cloud data warehouse (Silver/Gold layers) |

To deploy:
```bash
cd infrastructure
terraform init
terraform plan
terraform apply
```

> **Note**: Requires a GCP service account key at `infrastructure/keys/gcp-service-account.json`.

---

## Orchestration (Kestra)

The [`bronze_ingestion_flow.yml`](orchestration/bronze_ingestion_flow.yml) defines a Kestra flow with two tasks:

| Task ID | Description |
| ------- | ----------- |
| `run_extractions` | Installs Python packages, seeds Postgres, runs all extraction scripts, and executes DuckDB transforms |
| `run_dbt_transformations` | Installs `dbt-duckdb`, navigates to the dbt project, and runs `dbt build` |

The Kestra container mounts `ingestion/` and `warehouse/` as volumes so scripts are accessible at `/app/ingestion` and `/app/warehouse` respectively.

---

## Services & Ports

| Service       | URL                        | Credentials                              |
| ------------- | -------------------------- | ---------------------------------------- |
| **PostgreSQL** | `localhost:5434`          | `farm_admin` / `farm_password`           |
| **pgAdmin**   | `http://localhost:8080`    | `admin@farm.com` / `admin`               |
| **MinIO Console** | `http://localhost:9001` | `minio_admin` / `minio_password`         |
| **MinIO API** | `http://localhost:9000`    | `minio_admin` / `minio_password`         |
| **Kestra**    | `http://localhost:8082`    | No auth (local mode)                     |

---

## Data Sources

| Dataset | Source | Description |
| ------- | ------ | ----------- |
| Crop Yields | [FAOSTAT / Kaggle](https://www.kaggle.com/) | Historical crop yield data (hg/ha) for multiple crops |
| Weather Data | [Open-Meteo Historical API](https://open-meteo.com/) | Daily temperature and precipitation data (2010–2020) |
| Crop Categories | Manual seed file | Static mapping of crop types to categories |

---

## Environment Variables

| Variable            | Default            | Description                    |
| ------------------- | ------------------ | ------------------------------ |
| `POSTGRES_USER`     | `farm_admin`       | PostgreSQL username            |
| `POSTGRES_PASSWORD` | `farm_password`    | PostgreSQL password            |
| `POSTGRES_DB`       | `farm_management`  | PostgreSQL database name       |

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m 'feat: add your feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Open a Pull Request

---

## License

This project is open source and available for educational and research purposes.
