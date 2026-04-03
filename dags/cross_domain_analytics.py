"""DAG: Cross-Domain Analytics (Dataset-Triggered).

Automatically triggers when ALL three upstream DAGs complete, using
Airflow 2.4+ Dataset-aware scheduling. Produces cross-domain KPIs
that join tax, deforestation, and spending data.

This DAG has NO schedule — it fires only when the upstream Datasets
(irpf, deforestation, spending) are all updated.
"""

from datetime import datetime

from airflow.decorators import dag, task

from dags.common.constants import (
    DEFAULT_ARGS,
    DS_DEFORESTATION,
    DS_IRPF,
    DS_SPENDING,
    GOLD_PATH,
)
from dags.common.duckdb_operator import DuckDBOperator


@dag(
    dag_id="cross_domain_analytics",
    description="Cross-domain KPIs: tax vs spending vs deforestation",
    schedule=[DS_IRPF, DS_DEFORESTATION, DS_SPENDING],
    start_date=datetime(2000, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["cross-domain", "analytics", "kpi"],
    doc_md=__doc__,
)
def cross_domain_analytics():

    fiscal_balance = DuckDBOperator(
        task_id="build_fiscal_balance",
        sql_file="gold/cross_domain/fiscal_balance.sql",
        template_params={"gold_path": GOLD_PATH},
    )

    deforestation_vs_spending = DuckDBOperator(
        task_id="build_deforestation_vs_spending",
        sql_file="gold/cross_domain/deforestation_vs_spending.sql",
        template_params={"gold_path": GOLD_PATH},
    )

    @task(task_id="log_completion")
    def log_completion(**context):
        """Log that all cross-domain KPIs have been computed."""
        import duckdb
        from dags.common.constants import DUCKDB_PATH

        con = duckdb.connect(DUCKDB_PATH)

        # Create convenience views over the gold Parquet files
        views = {
            "gold_fiscal_balance": f"read_parquet('{GOLD_PATH}/cross_domain/fiscal_balance/**/*.parquet')",
            "gold_deforestation_vs_spending": f"read_parquet('{GOLD_PATH}/cross_domain/deforestation_vs_spending/**/*.parquet')",
            "gold_deforestation_by_biome": f"read_parquet('{GOLD_PATH}/deforestation_by_biome/**/*.parquet')",
            "gold_deforestation_by_state": f"read_parquet('{GOLD_PATH}/deforestation_by_state/**/*.parquet')",
            "gold_spending_by_function": f"read_parquet('{GOLD_PATH}/spending_by_function/**/*.parquet')",
            "gold_tax_by_bracket": f"read_parquet('{GOLD_PATH}/tax_by_bracket/**/*.parquet')",
        }

        for view_name, source in views.items():
            con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {source}")

        con.close()
        return {"status": "complete", "views_created": list(views.keys())}

    [fiscal_balance, deforestation_vs_spending] >> log_completion()


cross_domain_analytics()
