"""DAG: Government Spending Pipeline (SICONFI / Tesouro Nacional).

Ingests federal budget execution data from the SICONFI API and processes
it through Bronze → Silver → Gold layers using DuckDB SQL.

Schedule: Weekly (data updates monthly/annually).
Short-circuits if the source hasn't changed.

Data source: SICONFI API — https://apidatalake.tesouro.gov.br/ords/siconfi/
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import ShortCircuitOperator

from common.constants import (
    BRONZE_PATH,
    DEFAULT_ARGS,
    DS_SPENDING,
    GOLD_PATH,
    GOV_API_POOL,
    RAW_PATH,
    SILVER_PATH,
)
from common.duckdb_operator import DuckDBOperator
from common.quality_checks import run_quality_checks


@dag(
    dag_id="government_spending_pipeline",
    description="Federal spending by function: SICONFI → Bronze → Silver → Gold",
    schedule="@weekly",
    start_date=datetime(2000, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["spending", "siconfi", "budget", "government"],
    doc_md=__doc__,
)
def government_spending_pipeline():

    check_source = ShortCircuitOperator(
        task_id="check_source_changed",
        python_callable=_check_source_changed,
    )

    @task(task_id="fetch_siconfi_data", pool=GOV_API_POOL)
    def fetch_data(**context):
        import sys

        sys.path.insert(0, "/opt/airflow")
        from include.extractors.siconfi import fetch_spending_data

        return fetch_spending_data(
            f"{RAW_PATH}/spending", start_year=2000, end_year=2024
        )

    bronze = DuckDBOperator(
        task_id="bronze_ingest",
        sql_file="bronze/siconfi_ingest.sql",
        template_params={
            "source_file": f"{RAW_PATH}/spending/federal_spending.csv",
            "bronze_path": BRONZE_PATH,
        },
    )

    silver = DuckDBOperator(
        task_id="silver_clean",
        sql_file="silver/siconfi_clean.sql",
        template_params={"bronze_path": BRONZE_PATH, "silver_path": SILVER_PATH},
    )

    gold_function = DuckDBOperator(
        task_id="gold_spending_by_function",
        sql_file="gold/spending_by_function.sql",
        template_params={"silver_path": SILVER_PATH, "gold_path": GOLD_PATH},
    )

    @task(task_id="quality_checks")
    def quality_checks(**context):
        return run_quality_checks(
            [
                {
                    "name": "bronze_rows",
                    "sql": f"SELECT count(*) FROM read_parquet('{BRONZE_PATH}/spending/**/*.parquet')",
                    "op": "gt",
                    "threshold": 0,
                },
                {
                    "name": "silver_rows",
                    "sql": f"SELECT count(*) FROM read_parquet('{SILVER_PATH}/spending/**/*.parquet')",
                    "op": "gt",
                    "threshold": 0,
                },
                {
                    "name": "gold_rows",
                    "sql": f"SELECT count(*) FROM read_parquet('{GOLD_PATH}/spending_by_function/**/*.parquet')",
                    "op": "gt",
                    "threshold": 0,
                },
                {
                    "name": "no_negative_spending",
                    "sql": f"SELECT count(*) FROM read_parquet('{SILVER_PATH}/spending/**/*.parquet') WHERE executed_brl < 0",
                    "op": "eq",
                    "threshold": 0,
                },
            ]
        )

    @task(task_id="publish_dataset", outlets=[DS_SPENDING])
    def publish(**context):
        return {"status": "published", "dataset": "spending"}

    dl = fetch_data()
    (
        check_source
        >> dl
        >> bronze
        >> silver
        >> gold_function
        >> quality_checks()
        >> publish()
    )


def _check_source_changed(**context):
    from common.etag_checker import check_source_changed

    return check_source_changed(
        source_key="siconfi_spending",
        url="https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo",
    )


government_spending_pipeline()
