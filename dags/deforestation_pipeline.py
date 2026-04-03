"""DAG: PRODES Deforestation Pipeline.

Ingests annual deforestation data from INPE PRODES (TerraBrasilis) and
processes it through Bronze → Silver → Gold layers using DuckDB SQL.

Schedule: Weekly (data updates annually, but weekly checks for freshness).
Short-circuits if the source hasn't changed since the last run.

Data source: INPE PRODES — http://terrabrasilis.dpi.inpe.br/
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import ShortCircuitOperator

from dags.common.constants import (
    BRONZE_PATH,
    DEFAULT_ARGS,
    DS_DEFORESTATION,
    GOLD_PATH,
    GOV_API_POOL,
    RAW_PATH,
    SILVER_PATH,
)
from dags.common.duckdb_operator import DuckDBOperator
from dags.common.quality_checks import run_quality_checks


@dag(
    dag_id="deforestation_pipeline",
    description="INPE PRODES deforestation data: Bronze → Silver → Gold",
    schedule="@weekly",
    start_date=datetime(2000, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["deforestation", "prodes", "inpe", "environment"],
    doc_md=__doc__,
)
def deforestation_pipeline():

    check_source = ShortCircuitOperator(
        task_id="check_source_changed",
        python_callable=_check_source_changed,
    )

    @task(task_id="download_prodes", pool=GOV_API_POOL)
    def download_prodes(**context):
        """Download PRODES data from TerraBrasilis or fallback."""
        from include.extractors.prodes import download_prodes

        output_dir = f"{RAW_PATH}/prodes"
        filepath = download_prodes(output_dir)
        return filepath

    bronze = DuckDBOperator(
        task_id="bronze_ingest",
        sql_file="bronze/prodes_ingest.sql",
        template_params={
            "source_file": f"{RAW_PATH}/prodes/prodes_deforestation.csv",
            "bronze_path": BRONZE_PATH,
        },
    )

    silver = DuckDBOperator(
        task_id="silver_clean",
        sql_file="silver/prodes_clean.sql",
        template_params={
            "bronze_path": BRONZE_PATH,
            "silver_path": SILVER_PATH,
        },
    )

    gold_biome = DuckDBOperator(
        task_id="gold_deforestation_by_biome",
        sql_file="gold/deforestation_by_biome.sql",
        template_params={
            "silver_path": SILVER_PATH,
            "gold_path": GOLD_PATH,
        },
    )

    gold_state = DuckDBOperator(
        task_id="gold_deforestation_by_state",
        sql_file="gold/deforestation_by_state.sql",
        template_params={
            "silver_path": SILVER_PATH,
            "gold_path": GOLD_PATH,
        },
    )

    @task(task_id="quality_checks")
    def quality_checks(**context):
        """Run data quality assertions against all layers."""
        return run_quality_checks([
            {
                "name": "bronze_row_count",
                "sql": f"SELECT count(*) FROM read_parquet('{BRONZE_PATH}/prodes/**/*.parquet')",
                "op": "gt",
                "threshold": 0,
            },
            {
                "name": "silver_row_count",
                "sql": f"SELECT count(*) FROM read_parquet('{SILVER_PATH}/prodes/**/*.parquet')",
                "op": "gt",
                "threshold": 0,
            },
            {
                "name": "silver_no_null_years",
                "sql": f"SELECT count(*) FROM read_parquet('{SILVER_PATH}/prodes/**/*.parquet') WHERE year IS NULL",
                "op": "eq",
                "threshold": 0,
            },
            {
                "name": "silver_no_negative_areas",
                "sql": f"SELECT count(*) FROM read_parquet('{SILVER_PATH}/prodes/**/*.parquet') WHERE area_km2 < 0",
                "op": "eq",
                "threshold": 0,
            },
            {
                "name": "gold_biome_has_data",
                "sql": f"SELECT count(*) FROM read_parquet('{GOLD_PATH}/deforestation_by_biome/**/*.parquet')",
                "op": "gt",
                "threshold": 0,
            },
        ])

    @task(task_id="publish_dataset", outlets=[DS_DEFORESTATION])
    def publish(**context):
        """Mark the deforestation dataset as updated for downstream DAGs."""
        return {"status": "published", "dataset": "deforestation"}

    # DAG flow
    dl = download_prodes()
    check_source >> dl >> bronze >> silver >> [gold_biome, gold_state] >> quality_checks() >> publish()


def _check_source_changed(**context):
    """Check if PRODES data has changed since last run."""
    from dags.common.etag_checker import check_source_changed

    return check_source_changed(
        source_key="prodes_deforestation",
        url="https://terrabrasilis.dpi.inpe.br/downloads/prodes/prodes_rates_legal_amazon_biome.csv",
    )


deforestation_pipeline()
