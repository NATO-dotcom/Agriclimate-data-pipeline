# 🌾 Agri-Climate Data Pipeline

A **zero-cost, fully local** ELT data pipeline that ingests agricultural crop-yield data and historical climate data for **East Africa** (Kenya, Rwanda, Uganda, Tanzania), transforms it through a **Medallion Architecture** (Bronze → Silver → Gold), and produces an analytics-ready **Climate Impact Analysis** dataset — all orchestrated with **Kestra**.

> **New in v2**: Apache Spark batch processing layer, Dockerized Spark jobs with MinIO upload, and scheduled Kestra orchestration for containerized Spark pipelines.

### Why ELT over ETL?

This pipeline follows the **ELT (Extract-Load-Transform)** pattern rather than traditional ETL. Raw data is first loaded as-is into the MinIO data lake (the "Load" step), and transformations happen _after_ landing — inside DuckDB, dbt, and Spark. This approach:
- **Preserves raw data** — the Bronze layer is an immutable audit trail.
- **Decouples ingestion from transformation** — you can re-transform without re-extracting.
- **Leverages modern OLAP engines** — DuckDB and Spark are far more powerful than pre-load transform scripts.

### What is the Medallion Architecture?

The Medallion Architecture is a data design pattern that organizes data into three progressive quality layers:

| Layer | Purpose | Data Quality |
| ----- | ------- | ------------ |
| **🥉 Bronze** | Raw, unmodified data exactly as extracted from sources | Low — raw, potentially messy |
| **🥈 Silver** | Cleaned, deduplicated, denormalized, and aggregated data | Medium — validated and structured |
| **🥇 Gold** | Business-level analytics tables ready for dashboards and reporting | High — joined, enriched, analytics-ready |

This separation ensures that each layer has a clear responsibility and that upstream issues don't corrupt downstream analytics.

---

## 📑 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [What's New — Features Added](#whats-new--features-added)
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
- [Spark Batch Processing](#spark-batch-processing)
  - [Job 1 — Schema Enforcement & Parquet Conversion](#job-1--schema-enforcement--parquet-conversion)
  - [Job 2 — Broadcast Join & Aggregation](#job-2--broadcast-join--aggregation)
  - [Job 3 — MinIO Upload Pipeline](#job-3--minio-upload-pipeline)
  - [Dockerized Spark Execution](#dockerized-spark-execution)
- [dbt Project — agri_climate_models](#dbt-project--agri_climate_models)
  - [Staging Models](#staging-models)
  - [Mart Models](#mart-models)
  - [Seeds](#seeds)
  - [Macros](#macros)
  - [Tests](#tests)
  - [Snapshots](#snapshots)
- [Infrastructure (Terraform)](#infrastructure-terraform)
- [Orchestration (Kestra)](#orchestration-kestra)
  - [Bronze Ingestion Flow](#bronze-ingestion-flow)
  - [Spark Batch Flow](#spark-batch-flow)
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
4. **Batch processing** with **Apache Spark** — enforcing strict schemas, performing broadcast joins, aggregating yields by region, and uploading Gold-layer Parquet results to MinIO.
5. **Orchestrating** the entire flow end-to-end with **Kestra**, including a daily-scheduled Dockerized Spark pipeline.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           ORCHESTRATION (Kestra)                             │
│  bronze_ingestion_flow.yml — ELT extraction, DuckDB transforms, dbt build   │
│  spark_batch_flow.yaml    — Dockerized Spark batch job (daily cron)          │
└──────────────────┬───────────────────────────┬───────────────────────────────┘
                   │                           │
    ┌──────────────┼──────────────┐            │
    ▼              ▼              ▼            ▼
┌──────────┐ ┌───────────┐ ┌──────────┐ ┌───────────────────┐
│ seed_db  │ │ Open-Meteo │ │ Kaggle / │ │  PySpark Engine   │
│(Postgres)│ │   API      │ │ FAOSTAT  │ │ (Dockerized)      │
└────┬─────┘ └─────┬──────┘ └──────────┘ └─────┬─────────────┘
     │             │                           │
     ▼             ▼                           ▼
┌──────────────────────────────────────────────────────────────┐
│              MinIO Data Lake (S3-compatible)                  │
│                                                              │
│  agri-data-lake/                                             │
│  ├── bronze/                                                 │
│  │   ├── crop_data/                                          │
│  │   │   ├── regions.parquet                                 │
│  │   │   ├── fields.parquet                                  │
│  │   │   └── harvest_yields.parquet                          │
│  │   └── climate_data/                                       │
│  │       └── historical_weather.parquet                      │
│  ├── silver/                                                 │
│  │   ├── denormalized_crops.parquet                          │
│  │   └── yearly_weather.parquet                              │
│  └── gold/                                                   │
│      └── climate_impact_analysis.parquet                     │
│                                                              │
│  agri-data/                                                  │
│  └── gold_yield_report.parquet  ← Spark batch output         │
└──────────────────────────────────────────────────────────────┘
     │
     ▼
┌──────────────────────────────────────────────────────────────┐
│              dbt + DuckDB Warehouse                          │
│   staging (views)  →  marts (tables)  →  snapshots (SCD)     │
│   stg_crops, stg_weather →                                   │
│        climate_impact_analysis (+ crop_categories seed)       │
└──────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer             | Technology                                                                                     |
| ----------------- | ---------------------------------------------------------------------------------------------- |
| **Source DB**      | PostgreSQL 15 (Dockerized)                                                                     |
| **Data Lake**     | MinIO (S3-compatible object storage, Dockerized)                                               |
| **Batch Processing** | Apache Spark / PySpark 4.2.0 (local mode + Dockerized)                                     |
| **Transforms**    | DuckDB (in-process OLAP engine) + dbt-duckdb                                                  |
| **Orchestration** | Kestra (workflow orchestrator, Dockerized, with cron scheduling)                               |
| **DB Admin UI**   | pgAdmin 4 (Dockerized)                                                                         |
| **Data Format**   | Apache Parquet (via PyArrow + Snappy compression)                                              |
| **Language**      | Python 3.14 (managed with `uv`)                                                               |
| **Containerization** | Docker + Docker Compose                                                                    |
| **IaC**           | Terraform (Google Cloud — GCS bucket + BigQuery dataset for optional cloud deployment)         |

---

## What's New — Features Added

The following features have been added beyond the initial Bronze ingestion pipeline:

### 🔥 Apache Spark Batch Processing (`spark_jobs/`)
- **`01_first_look.py`** — Strict schema enforcement with `StructType`, CSV-to-Parquet conversion with repartitioning for optimized parallel reads.
- **`02_transform_join.py`** — Broadcast join between large Parquet yield data and a small CSV region lookup, wide aggregation (GroupBy + sum/count), and Gold-layer Parquet output with interactive Spark UI support.
- **`03_minio_pipeline.py`** — End-to-end pipeline: reads local Parquet, performs broadcast join + aggregation, converts to Pandas for single-file output, and uploads the Gold-layer report to MinIO via native `boto3`.

### 🐳 Dockerized Spark Execution
- **`Dockerfile`** — Production-ready container image (`python:3.11-slim` + OpenJDK) that bundles PySpark, project dependencies (via `uv`), region lookup data, and the Spark-to-MinIO pipeline script.
- Self-contained execution: `docker build` + `docker run` runs the full Spark pipeline with zero host-side dependencies.

### ⏰ Spark Batch Orchestration (`orchestration/spark_batch_flow.yaml`)
- **Kestra Docker plugin** (`io.kestra.plugin.docker.Run`) runs the containerized Spark job.
- **Daily cron schedule** (`0 0 * * *`) for automated nightly batch processing.
- Uses `networkMode: host` so the Spark container can reach the local MinIO instance.

### 🔄 dbt Snapshots — Slowly Changing Dimensions (`warehouse/agri_climate_models/snapshots/`)
- **`fields_snapshot.sql`** — Tracks historical changes to field metadata (e.g., `soil_type` changes) using dbt's `check` strategy with SCD Type 2 behavior.

### 🧪 dbt Testing & Quality
- **Schema tests**: `not_null` constraints on `region_id`, `harvest_year`, `total_rainfall_mm`.
- **Singular test**: `assert_positive_metrics.sql` — validates that crop yields and rainfall values are never negative.

### 🌱 dbt Seeds & Macros
- **`crop_categories.csv`** — Static lookup table mapping crop types to categories (Cereal, Legume, Tuber) for enriched analytics.
- **`convert_kg_to_tons` macro** — Reusable Jinja2 function for consistent unit conversion across all models.

### 🎯 Full ELT Orchestration with Kestra
- **`bronze_ingestion_flow.yml`** — Complete Kestra flow automating: DB seeding → Postgres extraction → Weather API ingestion → DuckDB Silver/Gold transforms → dbt build (staging + marts).
- Docker volume mounts for `ingestion/` and `warehouse/` directories.

---

## Project Structure

```
agri-climate-pipeline/
│
├── .env                             # Environment variables (Postgres credentials)
├── .gitignore                       # Ignores keys, .env, CSVs, Terraform state
├── .python-version                  # Python version (3.14)
├── pyproject.toml                   # Project metadata & dependencies (uv-managed)
├── uv.lock                          # Lockfile for deterministic installs
├── Dockerfile                       # Spark batch container (Python + JDK + PySpark)
├── region_lookup.csv                # Region → climate zone mapping (Spark join input)
├── yield.csv                        # Small yield dataset (Spark schema enforcement input)
├── README.md                        # ← You are here
│
├── docker/
│   ├── docker-compose.yml           # Spins up Postgres, pgAdmin, MinIO, Kestra
│   └── logs/
│       └── dbt.log                  # Docker-context dbt logs
│
├── infrastructure/
│   ├── main.tf                      # Terraform: GCS bucket + BigQuery dataset
│   ├── variables.tf                 # Terraform: project_id, region
│   ├── .terraform.lock.hcl          # Provider lock file
│   └── keys/                        # GCP service account key (git-ignored)
│
├── ingestion/
│   ├── seed_db.py                   # Seeds Postgres with FAOSTAT crop-yield data
│   ├── extract_postgres_to_minio.py # Extracts Postgres tables → MinIO Parquet
│   ├── extract_weather_to_minio.py  # Fetches Open-Meteo API → MinIO Parquet
│   ├── transform_duckdb.py          # DuckDB SQL transforms (Bronze → Silver → Gold)
│   ├── yield.csv                    # FAOSTAT crop yield dataset (git-ignored)
│   ├── yield_df.csv                 # Processed yield data
│   ├── pesticides.csv               # Pesticide usage dataset
│   ├── rainfall.csv                 # Rainfall dataset
│   └── temp.csv                     # Temperature dataset
│
├── spark_jobs/
│   ├── 01_first_look.py             # Schema enforcement + CSV → Parquet conversion
│   ├── 02_transform_join.py         # Broadcast join + aggregation + Gold report
│   └── 03_minio_pipeline.py         # Full pipeline: join → aggregate → MinIO upload
│
├── spark_output/
│   ├── yields_parquet/              # Partitioned Parquet output from Job 01
│   ├── gold_yield_report/           # Partitioned Parquet output from Job 02
│   └── gold_yield_report.parquet    # Single-file Parquet from Job 03
│
├── orchestration/
│   ├── bronze_ingestion_flow.yml    # Kestra: full ELT pipeline (seed → extract → transform → dbt)
│   └── spark_batch_flow.yaml        # Kestra: Dockerized Spark batch job (daily cron)
│
├── warehouse/
│   └── agri_climate_models/         # dbt project root
│       ├── dbt_project.yml          # dbt project configuration
│       ├── profiles.yml             # dbt profile (DuckDB + MinIO S3 connection)
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml      # External source definitions (MinIO Parquet)
│       │   │   ├── schema.yml       # Column-level tests & documentation
│       │   │   ├── stg_crops.sql    # Denormalized crop yields view
│       │   │   └── stg_weather.sql  # Aggregated annual weather view
│       │   └── marts/
│       │       └── climate_impact_analysis.sql  # Final analytics table
│       ├── seeds/
│       │   └── crop_categories.csv  # Static lookup: crop → category mapping
│       ├── macros/
│       │   └── convert_kg_to_tons.sql  # Reusable Jinja macro
│       ├── tests/
│       │   └── assert_positive_metrics.sql  # Singular test: no negative values
│       ├── snapshots/
│       │   └── fields_snapshot.sql  # SCD Type 2: tracks field metadata changes
│       ├── analyses/                # Ad-hoc analyses (placeholder)
│       └── target/                  # dbt build artifacts (auto-generated)
│
└── logs/
    └── dbt.log                      # Local dbt execution logs
```

---

## Getting Started

### Prerequisites

| Tool              | Version  | Purpose                                   |
| ----------------- | -------- | ----------------------------------------- |
| Docker            | 20+      | Containerized services                    |
| Docker Compose    | v2+      | Multi-container orchestration             |
| Python            | 3.10+    | Ingestion & transform scripts             |
| uv                | latest   | Fast Python package manager               |
| Java (JDK/JRE)    | 11+      | Required by PySpark (host-mode execution) |
| Terraform         | 1.5+     | *(Optional)* Cloud infrastructure         |

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

#### Option C — Spark Batch Processing

```bash
# Run Spark jobs locally (requires Java installed)
uv run python spark_jobs/01_first_look.py
uv run python spark_jobs/02_transform_join.py
uv run python spark_jobs/03_minio_pipeline.py
```

#### Option D — Dockerized Spark (No Java Required on Host)

```bash
# Build the Spark container
docker build -t agriclimate-spark:latest .

# Run the containerized pipeline
docker run --network host agriclimate-spark:latest
```

---

## Pipeline Stages

### 1. Source System — PostgreSQL

[`seed_db.py`](ingestion/seed_db.py) reads the raw FAOSTAT CSV and **normalizes** it into three relational tables. Normalization is the process of splitting a single flat CSV into multiple related tables to eliminate data redundancy and enforce referential integrity.

| Table              | Description                                              | Key |
| ------------------ | -------------------------------------------------------- | --- |
| `regions`          | East African countries with lat/lon and climate zones    | `region_id` (PK) |
| `fields`           | Crop-specific production fields linked to regions        | `field_id` (PK), `region_id` (FK) |
| `harvest_yields`   | Yearly yield records (kg) per field and crop type        | `harvest_id` (PK), `field_id` (FK) |

**Target countries**: Kenya, Rwanda, Uganda, Tanzania

**How the seeding works:**
1. Reads the raw FAOSTAT CSV and filters for East African countries only.
2. Maps each country to its approximate **agricultural centroid coordinates** (lat/lon) and assigns climate zones:
   - Kenya → Equatorial (`-0.02°, 37.91°`)
   - Rwanda → Highland Tropical (`-1.94°, 29.87°`)
   - Uganda → Tropical (`1.37°, 32.29°`)
   - Tanzania → Sub-Tropical (`-6.37°, 34.89°`)
3. Generates structured IDs (`REG_001`, `FLD_001`, etc.) for traceability.
4. Converts yield from `hg/ha` (hectograms per hectare) to `kg/ha` using a `× 0.1` factor.
5. Drops and recreates all tables with `CASCADE` to ensure idempotent re-runs.
6. Adds `PRIMARY KEY` constraints after loading via `ALTER TABLE`.

### 2. Bronze Layer — Raw Ingestion

| Script                                                                       | What it does                                                                                    |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| [`extract_postgres_to_minio.py`](ingestion/extract_postgres_to_minio.py)     | Reads all 3 Postgres tables → converts to Parquet (via PyArrow) → uploads to `s3://agri-data-lake/bronze/crop_data/` |
| [`extract_weather_to_minio.py`](ingestion/extract_weather_to_minio.py)       | Queries the Open-Meteo Historical API (2010–2020) for daily max/min temp & precipitation per region → uploads to `s3://agri-data-lake/bronze/climate_data/` |

**Key implementation details:**

- **In-memory Parquet buffer**: Both scripts use `io.BytesIO()` as an in-memory buffer — DataFrames are written to Parquet in RAM and then uploaded directly to MinIO via `s3_client.upload_fileobj()`, avoiding temporary files on disk.
- **Bucket auto-creation**: Both scripts call `s3_client.head_bucket()` to check if the bucket exists, and create it via `s3_client.create_bucket()` if not — making the pipeline idempotent.
- **MinIO as S3-compatible storage**: MinIO is accessed using the standard AWS `boto3` SDK by overriding `endpoint_url` to point to the local MinIO container (`http://minio:9000`). This means the same code would work with real AWS S3 by simply changing the endpoint.
- **Weather API rate-limiting**: A 1-second `time.sleep()` pause between region requests respects the free-tier Open-Meteo API limits.
- **Weather variables fetched**: `temperature_2m_max`, `temperature_2m_min`, `precipitation_sum` (daily granularity, `Africa/Nairobi` timezone).

### 3. Silver Layer — Cleaning & Aggregation

[`transform_duckdb.py`](ingestion/transform_duckdb.py) connects DuckDB directly to MinIO and performs in-process SQL transformations without needing a running database server.

**DuckDB S3 Secret Configuration:**

DuckDB connects to MinIO using the `httpfs` and `aws` extensions with a `CREATE SECRET` statement that configures:
- `ENDPOINT` → `minio:9000` (Docker container hostname)
- `USE_SSL` → `false` (local development, no TLS)
- `URL_STYLE` → `path` (MinIO uses path-style URLs: `endpoint/bucket/key` rather than `bucket.endpoint/key`)

This allows DuckDB to use `read_parquet('s3://...')` and `COPY ... TO 's3://...'` syntax to read from and write to MinIO as if it were AWS S3.

**Transformations performed:**

- **Yearly weather aggregation**: Daily weather → annual totals/averages per region using `SUM(precipitation_sum)` and `AVG(temperature_2m_max)`, grouped by `region_id` and `harvest_year`.
- **Crop denormalization**: A 3-table join (`harvest_yields` ← `fields` ← `regions`) collapses the normalized relational schema into one wide analytical table, converting `yield_kg` to `yield_tons` (÷ 1000).

Output: `s3://agri-data-lake/silver/yearly_weather.parquet` and `s3://agri-data-lake/silver/denormalized_crops.parquet`

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

## Spark Batch Processing

The `spark_jobs/` directory contains a progressive 3-step PySpark processing layer that runs independently of the DuckDB/dbt pipeline. While DuckDB handles the ELT transforms on MinIO data, the Spark layer demonstrates **distributed batch processing** concepts on a separate dataset (`yield.csv` + `region_lookup.csv` at the project root).

**Why Spark alongside DuckDB?** DuckDB excels at single-node analytical queries on Parquet files in a data lake. Spark is designed for distributed processing across clusters — this project uses Spark in `local[*]` mode (all CPU cores) to demonstrate production-ready patterns (schema enforcement, broadcast joins, partitioned writes) that scale horizontally when moved to a cluster.

### Job 1 — Schema Enforcement & Parquet Conversion

**Script**: [`01_first_look.py`](spark_jobs/01_first_look.py)

| Feature | Detail |
| ------- | ------ |
| **Schema enforcement** | Custom `StructType` schema with explicit types (`IntegerType`, `StringType`, `DoubleType`) — no `inferSchema` guessing |
| **Repartitioning** | Repartitions data into 2 partitions for balanced parallel reads |
| **Output** | `spark_output/yields_parquet/` (partitioned Snappy-compressed Parquet) |

> **Why enforce schemas?** Using `inferSchema=true` causes Spark to scan the entire file to guess column types — this is slow and error-prone (e.g., an ID column like `"001"` might be inferred as an integer, losing the leading zero). Defining a `StructType` upfront guarantees type safety and is the production best practice.

> **Why repartition?** `df.repartition(2)` redistributes data into 2 equal partitions before writing. Each partition becomes a separate Parquet file, enabling parallel reads by downstream consumers. The number is kept small here because the dataset is small.

### Job 2 — Broadcast Join & Aggregation

**Script**: [`02_transform_join.py`](spark_jobs/02_transform_join.py)

| Feature | Detail |
| ------- | ------ |
| **Broadcast join** | Uses `F.broadcast()` to push the small `region_lookup.csv` to all executor nodes (avoids expensive shuffle) |
| **Wide aggregation** | `GroupBy(region_name, climate_zone)` → `SUM(yield_tons)`, `COUNT(*)` |
| **Spark UI** | Keeps the SparkSession alive at `http://localhost:4040` for interactive DAG inspection |
| **Output** | `spark_output/gold_yield_report/` (partitioned Parquet) |

> **What is a Broadcast Join?** In a standard Spark join, both datasets are shuffled across the network so matching keys land on the same node — this is expensive. A **broadcast join** (`F.broadcast()`) sends the _entire_ small dataset (the region lookup CSV, ~117 bytes) to every executor node's memory. The large dataset (yields) stays in place and each node performs a local join — no shuffle needed. This is optimal when one side of the join is small enough to fit in memory.

> **Wide vs Narrow Transformations**: The `GroupBy` aggregation in this job is a **wide transformation** — it requires a data shuffle because records with the same key (`region_name`) may be on different partitions and must be gathered together. In contrast, operations like `filter` or `map` are **narrow transformations** that operate on each partition independently without network traffic.

### Job 3 — MinIO Upload Pipeline

**Script**: [`03_minio_pipeline.py`](spark_jobs/03_minio_pipeline.py)

| Feature | Detail |
| ------- | ------ |
| **Dynamic path resolution** | Uses `pathlib.Path` to resolve paths relative to the script, works in both local and Docker contexts |
| **Pandas bridge** | Converts the Spark DataFrame to Pandas for single-file Parquet output |
| **MinIO upload** | Creates the `agri-data` bucket if missing, uploads `gold_yield_report.parquet` via native `boto3` |
| **Output** | `spark_output/gold_yield_report.parquet` (local) + `s3://agri-data/gold_yield_report.parquet` (MinIO) |

### Dockerized Spark Execution

The project includes a production-ready [`Dockerfile`](Dockerfile) for containerized Spark execution:

```dockerfile
FROM python:3.11-slim         # Lightweight base image
RUN apt-get install -y default-jre  # Spark requires Java
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/  # Ultra-fast package manager
RUN uv pip install --system -r pyproject.toml             # Install all dependencies
CMD ["python", "/opt/pipeline/spark_jobs/03_minio_pipeline.py"]
```

**Build & Run:**
```bash
docker build -t agriclimate-spark:latest .
docker run --network host agriclimate-spark:latest
```

> The `--network host` flag allows the container to reach the MinIO instance running on the host at `127.0.0.1:9000`.

---

## dbt Project — `agri_climate_models`

The dbt layer re-implements the Silver → Gold transformations with full software engineering practices: version control, testing, documentation, snapshots, and modularity.

**Why dbt alongside DuckDB raw SQL?** The `transform_duckdb.py` script handles the same Silver → Gold logic, but as raw Python + SQL. dbt adds:
- **Modularity** — Each transformation is a separate `.sql` file with a clear dependency graph (`ref()` and `source()`).
- **Testing** — Schema tests (`not_null`) and singular tests (`assert_positive_metrics`) catch data quality issues before they reach dashboards.
- **Documentation** — `schema.yml` provides column-level documentation that auto-generates a data catalog.
- **Reproducibility** — `dbt build` runs seeds, models, snapshots, and tests in the correct dependency order every time.
- **Snapshots** — Track historical changes (SCD Type 2) that raw SQL cannot do declaratively.

**Profile**: DuckDB with MinIO S3 integration (via `httpfs` + `aws` extensions).

**External Sources via `meta.external_location`:**

The [`sources.yml`](warehouse/agri_climate_models/models/staging/sources.yml) file uses dbt-duckdb's `external_location` metadata to point dbt sources directly at Parquet files in MinIO:

```yaml
- name: weather
  meta:
    external_location: "read_parquet('s3://agri-data-lake/bronze/climate_data/*.parquet')"
```

This tells dbt-duckdb to replace `{{ source('bronze', 'weather') }}` with the `read_parquet(...)` call at compile time, allowing dbt to query Parquet files in MinIO as if they were database tables — no separate table creation step needed.

### Staging Models

| Model | Materialization | Description |
| ----- | --------------- | ----------- |
| [`stg_crops`](warehouse/agri_climate_models/models/staging/stg_crops.sql) | `view` | Denormalizes yields + fields + regions from Bronze Parquet sources. Uses the `convert_kg_to_tons` macro for unit conversion. |
| [`stg_weather`](warehouse/agri_climate_models/models/staging/stg_weather.sql) | `view` | Aggregates daily weather into yearly metrics per region (`SUM`, `AVG`). |

### Mart Models

| Model | Materialization | Description |
| ----- | --------------- | ----------- |
| [`climate_impact_analysis`](warehouse/agri_climate_models/models/marts/climate_impact_analysis.sql) | `table` | Joins staged crops + weather + `crop_categories` seed into the final analytics dataset with `crop_category` enrichment. |

### Seeds

| File | Purpose |
| ---- | ------- |
| [`crop_categories.csv`](warehouse/agri_climate_models/seeds/crop_categories.csv) | Static lookup table mapping crop types to categories: Maize/Wheat/Rice → Cereal, Beans → Legume, Potatoes/Cassava → Tuber |

### Macros

| Macro | Purpose |
| ----- | ------- |
| [`convert_kg_to_tons`](warehouse/agri_climate_models/macros/convert_kg_to_tons.sql) | Reusable Jinja macro: `(column_name / 1000.0)` for unit conversion from kg to metric tons |

### Tests

| Type | Test | Description |
| ---- | ---- | ----------- |
| **Schema** | `not_null` on `region_id`, `harvest_year` (stg_crops) | Ensures key columns are never null |
| **Schema** | `not_null` on `region_id`, `total_rainfall_mm` (stg_weather) | Ensures weather metrics are complete |
| **Singular** | [`assert_positive_metrics`](warehouse/agri_climate_models/tests/assert_positive_metrics.sql) | Validates that crop yields and rainfall values are never negative. Returns failing rows — if any are returned, the pipeline halts. |

### Snapshots

| Snapshot | Strategy | Tracked Columns | Description |
| -------- | -------- | --------------- | ----------- |
| [`fields_snapshot`](warehouse/agri_climate_models/snapshots/fields_snapshot.sql) | `check` | `soil_type` | SCD Type 2 tracking on the `fields` source table. Captures historical changes when `soil_type` is modified, preserving a full audit trail with `dbt_valid_from` and `dbt_valid_to` timestamps. |

> **What is SCD Type 2?** Slowly Changing Dimension (SCD) Type 2 is a data warehousing technique for tracking historical changes. Instead of overwriting a row when a value changes, a _new_ row is inserted with the updated value and a `dbt_valid_from` timestamp. The old row gets a `dbt_valid_to` timestamp marking when it stopped being current. This preserves a complete history — you can always answer "what was the soil type for field X on date Y?"

> **`check` vs `timestamp` strategy**: The `check` strategy compares the actual column values between runs to detect changes (no `updated_at` column needed). The `timestamp` strategy relies on an `updated_at` timestamp column in the source data. The `check` strategy is used here because the Bronze Parquet source doesn't have a reliable `updated_at` column.

---

## Infrastructure (Terraform)

The [`infrastructure/`](infrastructure/) directory provisions **optional** cloud resources on Google Cloud Platform using **Infrastructure as Code (IaC)**. This means cloud resources are defined in declarative `.tf` files, version-controlled in Git, and can be created, modified, or destroyed reproducibly.

| Resource | Name | Purpose |
| -------- | ---- | ------- |
| **GCS Bucket** | `{project_id}-agri-lake` | Cloud data lake (Bronze layer) |
| **BigQuery Dataset** | `agri_climate_warehouse` | Cloud data warehouse (Silver/Gold layers) |

**Provider**: `hashicorp/google` v5.6.0

**Lifecycle rules on the GCS bucket:**
- **30-day auto-delete**: Objects older than 30 days are automatically deleted — prevents development data from accumulating storage costs.
- **1-day abort incomplete multipart uploads**: Failed partial uploads are cleaned up after 1 day — a cost-saving best practice that prevents orphaned upload fragments from consuming storage.

To deploy:
```bash
cd infrastructure
terraform init    # Download the Google provider plugin
terraform plan    # Preview what will be created (dry run)
terraform apply   # Create the resources on GCP
```

> **Note**: Requires a GCP service account key at `infrastructure/keys/gcp-service-account.json`. This file is git-ignored for security.

---

## Orchestration (Kestra)

[Kestra](https://kestra.io/) is an open-source workflow orchestrator that coordinates multi-step data pipelines. It replaces manual script execution with declarative YAML flows, providing: retry logic, execution history, a visual web UI, cron scheduling, and Docker integration.

### Bronze Ingestion Flow

The [`bronze_ingestion_flow.yml`](orchestration/bronze_ingestion_flow.yml) defines a Kestra flow with two sequential tasks:

| Task ID | Type | Description |
| ------- | ---- | ----------- |
| `run_extractions` | `shell.Commands` | Installs Python packages (`pandas`, `sqlalchemy`, `psycopg2-binary`, `boto3`, `pyarrow`, `requests`, `duckdb`), seeds Postgres, runs all extraction scripts, and executes DuckDB Silver/Gold transforms |
| `run_dbt_transformations` | `shell.Commands` | Installs `dbt-duckdb`, configures `PYTHONPATH` and `PATH`, navigates to the dbt project directory, sets `DBT_PROFILES_DIR=$(pwd)`, and runs `dbt build` |

Both tasks use `runner: PROCESS` to execute commands directly inside the Kestra container (rather than spawning sub-containers). The Kestra container mounts:
- `../ingestion` → `/app/ingestion` — Python scripts and CSV data
- `../warehouse` → `/app/warehouse` — dbt project files

### Spark Batch Flow

The [`spark_batch_flow.yaml`](orchestration/spark_batch_flow.yaml) defines a separate Kestra flow for containerized Spark execution:

| Property | Value |
| -------- | ----- |
| **Flow ID** | `agriclimate_spark_batch` |
| **Namespace** | `dev.dinah` |
| **Plugin** | `io.kestra.plugin.docker.Run` |
| **Container Image** | `agriclimate-spark:latest` |
| **Network** | `host` (access to local MinIO) |
| **Schedule** | Daily at midnight (`0 0 * * *`) |

> **How Docker orchestration works here**: Unlike the Bronze flow (which runs shell commands inside the Kestra container), the Spark flow uses the Kestra Docker plugin to pull and run a _separate_ container (`agriclimate-spark:latest`). Kestra manages the container lifecycle — starting it, capturing logs, and reporting success/failure. The daily cron trigger (`0 0 * * *`) means Kestra automatically kicks off a fresh Spark batch run at midnight every day.

---

## Services & Ports

All services are defined in [`docker/docker-compose.yml`](docker/docker-compose.yml) and communicate over a shared Docker bridge network (`data_network`). This means containers refer to each other by **service name** (e.g., `farm_pg_source`, `minio`) rather than `localhost` — Docker's built-in DNS resolves these names to the correct container IPs.

| Service            | URL (from host)            | URL (from containers)      | Credentials                              |
| ------------------ | -------------------------- | -------------------------- | ---------------------------------------- |
| **PostgreSQL**     | `localhost:5434`           | `farm_pg_source:5432`      | `farm_admin` / `farm_password`           |
| **pgAdmin**        | `http://localhost:8080`    | —                          | `admin@farm.com` / `admin`               |
| **MinIO Console**  | `http://localhost:9001`    | —                          | `minio_admin` / `minio_password`         |
| **MinIO API**      | `http://localhost:9000`    | `http://minio:9000`        | `minio_admin` / `minio_password`         |
| **Kestra**         | `http://localhost:8082`    | —                          | No auth (local mode)                     |
| **Spark UI**       | `http://localhost:4040`    | —                          | No auth (available during Job 02 execution) |

> **Why port `5434` instead of `5432`?** PostgreSQL runs on port `5432` _inside_ its container, but is mapped to `5434` on the host to avoid conflicts with any local PostgreSQL installation.

> **Docker volumes**: `farm_pg_data` (named volume) persists PostgreSQL data across container restarts. `minio_data` persists the MinIO data lake. Both survive `docker compose down` but are removed by `docker compose down -v`.

---

## Data Sources

| Dataset | Source | File(s) | Description |
| ------- | ------ | ------- | ----------- |
| Crop Yields | [FAOSTAT / Kaggle](https://www.kaggle.com/) | `ingestion/yield.csv` | Historical crop yield data (hg/ha) for multiple crops across East Africa — the primary input for the PostgreSQL seeding pipeline |
| Weather Data | [Open-Meteo Historical API](https://open-meteo.com/) | *(fetched at runtime)* | Daily temperature (max/min) and precipitation data (2010–2020) for each region's centroid coordinates |
| Pesticide Usage | Kaggle | `ingestion/pesticides.csv` | Pesticide usage data by country — available as supplementary dataset for future analysis |
| Rainfall Data | Kaggle | `ingestion/rainfall.csv` | Historical rainfall records — supplementary dataset |
| Temperature Data | Kaggle | `ingestion/temp.csv` | Historical temperature records — supplementary dataset |
| Processed Yields | Derived | `ingestion/yield_df.csv` | Pre-processed yield data from earlier exploratory analysis |
| Crop Categories | Manual seed file | `warehouse/.../seeds/crop_categories.csv` | Static mapping: Maize/Wheat/Rice → Cereal, Beans → Legume, Potatoes/Cassava → Tuber |
| Region Lookup | Manual | `region_lookup.csv` | 3-region lookup (Rift Valley, Central Province, Meru County) with climate zones — used for Spark broadcast joins |
| Spark Yield Data | Manual | `yield.csv` (project root) | Small 4-row yield dataset for Spark schema enforcement demos |

---

## Environment Variables

| Variable            | Default            | Description                    |
| ------------------- | ------------------ | ------------------------------ |
| `POSTGRES_USER`     | `farm_admin`       | PostgreSQL username            |
| `POSTGRES_PASSWORD` | `farm_password`    | PostgreSQL password            |
| `POSTGRES_DB`       | `farm_management`  | PostgreSQL database name       |

**MinIO credentials** (hardcoded in scripts for local development):
| Variable              | Value              |
| --------------------- | ------------------ |
| `MINIO_ROOT_USER`     | `minio_admin`      |
| `MINIO_ROOT_PASSWORD` | `minio_password`   |

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m 'feat: add your feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Open a Pull Request

Please follow the existing commit convention:
- `feat:` — New feature
- `fix:` — Bug fix
- `docs:` — Documentation
- `chore:` — Maintenance / config
- `test:` — Tests

---

## License

This project is open source and available for educational and research purposes.
