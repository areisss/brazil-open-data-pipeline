# Brazil Open Data Pipeline

An Apache Airflow pipeline that ingests, processes, and analyzes 20+ years of Brazilian government data — tax revenue, Amazon deforestation, and federal spending — through a medallion architecture powered by DuckDB.

**Key principle:** Airflow orchestrates, DuckDB executes. No data processing in worker memory — all transformations are SQL pushed down to DuckDB, which reads and writes Parquet files directly.

```
                     ┌─────────────────────────┐
                     │   Apache Airflow         │
                     │   (orchestration only)   │
                     └───────────┬─────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
        ┌──────────┐     ┌────────────┐     ┌────────────┐
        │ IRPF DAG │     │ PRODES DAG │     │ Budget DAG │
        │ (Tax)    │     │ (Deforest.)│     │ (Spending) │
        └────┬─────┘     └─────┬──────┘     └─────┬──────┘
             │                 │                   │
             │     DuckDB SQL  │     DuckDB SQL    │
             ▼                 ▼                   ▼
        ┌──────────────────────────────────────────────┐
        │  Bronze → Silver → Gold (Partitioned Parquet)│
        └──────────────────┬───────────────────────────┘
                           │ Airflow Datasets
                           ▼
                 ┌──────────────────────┐
                 │ Cross-Domain DAG     │
                 │ (auto-triggers when  │
                 │  all 3 DAGs complete) │
                 └──────────────────────┘
```

---

## Data Sources

| Domain | Source | Range | API |
|---|---|---|---|
| **Income Tax** | Receita Federal (IRPF) | 2008-2024 | CSV download |
| **Deforestation** | INPE PRODES / TerraBrasilis | 2000-2024 | REST API / CSV |
| **Government Spending** | Tesouro Nacional (SICONFI) | 2000-2024 | REST API (JSON) |

---

## Features & Airflow Patterns

| Pattern | Implementation |
|---|---|
| **Data-aware scheduling** | `cross_domain_analytics` DAG triggers automatically via `Dataset` when all 3 source DAGs publish |
| **Short-circuit** | `ShortCircuitOperator` checks HTTP ETag — skips pipeline if source unchanged |
| **Custom operator** | `DuckDBOperator` executes SQL files in DuckDB (push compute down) |
| **Idempotent partitions** | Parquet partitioned by year; tasks overwrite only their partition |
| **Rate limiting** | `gov_api` pool limits concurrent API calls to 3 |
| **Quality gates** | Data quality checks (row counts, null rates, range assertions) between layers |
| **Retry with backoff** | Exponential backoff on API failures (3 retries, max 10 min) |
| **TaskFlow API** | `@task` decorator for Python tasks (Airflow 2.x modern pattern) |
| **Parameterized** | DAGs accept `params.year` for targeted backfills |

---

## KPIs Produced

### Tax
- Revenue by income bracket over time
- Effective tax rate by bracket
- Tax concentration (top vs bottom brackets)

### Deforestation
- Annual km² by biome (Amazon, Cerrado, etc.)
- Cumulative deforestation since 2000
- YoY acceleration/deceleration
- Top deforesting states per year

### Spending
- Federal spending by function (health, education, defense, etc.)
- Spending as % of total budget
- YoY nominal growth

### Cross-Domain
- Tax revenue vs government spending (fiscal balance)
- Deforestation vs environmental spending (investment vs outcome)

---

## Quick Start

```bash
# Clone
git clone https://github.com/areisss/brazil-open-data-pipeline.git
cd brazil-open-data-pipeline

# Start Airflow (Docker required)
make up

# Open Airflow UI
open http://localhost:8080   # admin / admin

# Trigger a DAG manually from the UI, or:
make backfill YEAR=2023 DAG=deforestation_pipeline

# Query results
duckdb warehouse/brazil_data.duckdb "SELECT * FROM gold_deforestation_by_biome LIMIT 10"

# Run tests
make test

# Tear down
make down
```

---

## Project Structure

```
brazil-open-data-pipeline/
├── dags/                              # Airflow DAG definitions
│   ├── irpf_tax_pipeline.py           # Tax data: Receita Federal → DuckDB
│   ├── deforestation_pipeline.py      # PRODES: TerraBrasilis → DuckDB
│   ├── government_spending_pipeline.py # SICONFI: Tesouro Nacional → DuckDB
│   ├── cross_domain_analytics.py      # Dataset-triggered cross-domain KPIs
│   └── common/
│       ├── duckdb_operator.py         # Custom operator: execute SQL in DuckDB
│       ├── etag_checker.py            # ETag/hash checker for ShortCircuit
│       ├── quality_checks.py          # DuckDB-based data quality assertions
│       └── constants.py               # Shared paths, Datasets, default args
│
├── sql/                               # All transformation SQL (not in Python)
│   ├── bronze/                        # Raw → Parquet ingestion
│   ├── silver/                        # Clean, normalize, validate
│   ├── gold/                          # KPI aggregations
│   │   └── cross_domain/             # Multi-source joins
│   └── quality/                       # Data quality check queries
│
├── include/extractors/                # Data source download logic
│   ├── irpf.py                        # Receita Federal CSV
│   ├── prodes.py                      # INPE TerraBrasilis
│   └── siconfi.py                     # SICONFI REST API
│
├── data/                              # Local data lake (partitioned Parquet)
│   ├── raw/                           # Downloaded CSV/JSON
│   ├── bronze/                        # year=YYYY/ partitions
│   ├── silver/                        # Cleaned, typed
│   └── gold/                          # Aggregated KPIs
│
├── warehouse/brazil_data.duckdb       # Analytics warehouse with views
├── tests/                             # DAG integrity + SQL validation tests
├── docker-compose.yml                 # Airflow + Postgres + Redis
├── Dockerfile                         # Custom image with DuckDB
└── Makefile                           # make up/down/test/backfill
```

---

## Design Decisions

| Decision | Why |
|---|---|
| **DuckDB over pandas** | Airflow workers don't hold data in memory. DuckDB reads/writes Parquet directly via SQL — scales to GB without OOM. |
| **SQL in files, not Python** | Transformations are reviewable, testable, and don't require Python expertise to modify. |
| **Datasets over Sensors** | Airflow 2.4+ Datasets are declarative and cleaner than `ExternalTaskSensor` polling. |
| **Partitioned Parquet** | Year-based partitions make backfills idempotent — reprocessing 2020 doesn't touch 2021. |
| **ETag short-circuit** | Government data updates infrequently. Weekly schedule + ETag check avoids redundant processing. |
| **Fallback data** | Extractors include compiled data from official reports so the pipeline demo works even if APIs are down. |

---

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 (CeleryExecutor) |
| Query engine | DuckDB 1.1 |
| Storage | Parquet (partitioned, local) |
| Infrastructure | Docker Compose |
| CI/CD | GitHub Actions |
| Testing | pytest |

---

## Cost

**$0** — everything runs locally in Docker. No cloud services required.
