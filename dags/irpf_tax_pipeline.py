"""DAG: IRPF Tax Revenue Pipeline (Receita Federal).

Ingests income tax declaration data by bracket from Receita Federal and
processes it through Bronze → Silver → Gold layers using DuckDB SQL.

Schedule: Weekly (data updates annually after filing season).
Short-circuits if the source hasn't changed.

Data source: Receita Federal — Grandes Numeros do IRPF
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import ShortCircuitOperator

from dags.common.constants import (
    BRONZE_PATH,
    DEFAULT_ARGS,
    DS_IRPF,
    GOLD_PATH,
    GOV_API_POOL,
    RAW_PATH,
    SILVER_PATH,
)
from dags.common.duckdb_operator import DuckDBOperator
from dags.common.quality_checks import run_quality_checks


@dag(
    dag_id="irpf_tax_pipeline",
    description="Income tax by bracket: Receita Federal → Bronze → Silver → Gold",
    schedule="@weekly",
    start_date=datetime(2008, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tax", "irpf", "receita-federal", "income"],
    doc_md=__doc__,
)
def irpf_tax_pipeline():

    check_source = ShortCircuitOperator(
        task_id="check_source_changed",
        python_callable=_check_source_changed,
    )

    @task(task_id="download_irpf", pool=GOV_API_POOL)
    def download(**context):
        from include.extractors.irpf import download_irpf
        return download_irpf(f"{RAW_PATH}/irpf")

    bronze = DuckDBOperator(
        task_id="bronze_ingest",
        sql_file="bronze/irpf_ingest.sql",
        template_params={
            "source_file": f"{RAW_PATH}/irpf/irpf_by_bracket.csv",
            "bronze_path": BRONZE_PATH,
        },
    )

    silver = DuckDBOperator(
        task_id="silver_clean",
        sql_file="silver/irpf_clean.sql",
        template_params={"bronze_path": BRONZE_PATH, "silver_path": SILVER_PATH},
    )

    gold = DuckDBOperator(
        task_id="gold_tax_by_bracket",
        sql_file="gold/tax_by_bracket.sql",
        template_params={"silver_path": SILVER_PATH, "gold_path": GOLD_PATH},
    )

    @task(task_id="quality_checks")
    def quality_checks(**context):
        return run_quality_checks([
            {"name": "bronze_rows", "sql": f"SELECT count(*) FROM read_parquet('{BRONZE_PATH}/irpf/**/*.parquet')", "op": "gt", "threshold": 0},
            {"name": "silver_rows", "sql": f"SELECT count(*) FROM read_parquet('{SILVER_PATH}/irpf/**/*.parquet')", "op": "gt", "threshold": 0},
            {"name": "gold_rows", "sql": f"SELECT count(*) FROM read_parquet('{GOLD_PATH}/tax_by_bracket/**/*.parquet')", "op": "gt", "threshold": 0},
            {"name": "no_negative_tax", "sql": f"SELECT count(*) FROM read_parquet('{SILVER_PATH}/irpf/**/*.parquet') WHERE tax_due_brl < 0", "op": "eq", "threshold": 0},
        ])

    @task(task_id="publish_dataset", outlets=[DS_IRPF])
    def publish(**context):
        return {"status": "published", "dataset": "irpf"}

    dl = download()
    check_source >> dl >> bronze >> silver >> gold >> quality_checks() >> publish()


def _check_source_changed(**context):
    from dags.common.etag_checker import check_source_changed
    return check_source_changed(
        source_key="irpf_data",
        url="https://www.gov.br/receitafederal/dados/grandes-numeros-irpf.csv",
    )


irpf_tax_pipeline()
