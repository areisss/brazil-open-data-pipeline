"""Shared constants for all DAGs."""

import os

from airflow.datasets import Dataset

# Paths (set via docker-compose environment, with local fallbacks)
DATA_LAKE = os.environ.get("DATA_LAKE_PATH", "/opt/airflow/data")
WAREHOUSE = os.environ.get("WAREHOUSE_PATH", "/opt/airflow/warehouse")
SQL_PATH = os.environ.get("SQL_PATH", "/opt/airflow/sql")

RAW_PATH = f"{DATA_LAKE}/raw"
BRONZE_PATH = f"{DATA_LAKE}/bronze"
SILVER_PATH = f"{DATA_LAKE}/silver"
GOLD_PATH = f"{DATA_LAKE}/gold"

DUCKDB_PATH = f"{WAREHOUSE}/brazil_data.duckdb"

# Airflow Datasets — used for data-aware scheduling between DAGs
DS_IRPF = Dataset("file://gold/irpf")
DS_DEFORESTATION = Dataset("file://gold/deforestation")
DS_SPENDING = Dataset("file://gold/spending")

# Pool for rate-limiting government API calls
GOV_API_POOL = "gov_api"

# Default DAG args
DEFAULT_ARGS = {
    "owner": "artur.reis",
    "retries": 3,
    "retry_delay": 60,  # seconds
    "retry_exponential_backoff": True,
    "max_retry_delay": 600,  # 10 minutes
}
