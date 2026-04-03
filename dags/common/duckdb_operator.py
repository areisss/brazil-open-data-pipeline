"""Custom Airflow operator that executes SQL in DuckDB.

Airflow orchestrates; DuckDB does the heavy lifting. The operator reads a .sql
file from the sql/ directory, renders Jinja templates ({{ params.year }}, etc.),
and executes it against the shared DuckDB database. No data passes through
Airflow worker memory — DuckDB reads/writes Parquet files directly.
"""

import os
from pathlib import Path

import duckdb
from airflow.models import BaseOperator
from airflow.utils.context import Context

from dags.common.constants import DUCKDB_PATH, SQL_PATH


class DuckDBOperator(BaseOperator):
    """Execute a SQL file or inline SQL in DuckDB.

    Parameters
    ----------
    sql_file : str, optional
        Path relative to SQL_PATH (e.g., "bronze/prodes_ingest.sql").
    sql : str, optional
        Inline SQL string. Use sql_file for anything non-trivial.
    template_params : dict, optional
        Extra Jinja parameters available as {{ params.key }} in SQL.
    """

    template_fields = ("sql", "sql_file", "template_params")

    def __init__(
        self,
        sql_file: str | None = None,
        sql: str | None = None,
        template_params: dict | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql_file = sql_file
        self.sql = sql
        self.template_params = template_params or {}

    def execute(self, context: Context) -> dict:
        sql = self._resolve_sql(context)
        self.log.info("Executing DuckDB SQL (%d chars)", len(sql))

        os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

        con = duckdb.connect(DUCKDB_PATH)
        try:
            # Enable Parquet and httpfs extensions for reading remote/local files
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except duckdb.IOException:
            pass  # Already installed

        try:
            result = con.execute(sql)
            description = result.description if result.description else []
            rows_changed = result.fetchone()[0] if description else 0
        except Exception:
            # DDL statements (COPY, CREATE TABLE) don't return rows
            rows_changed = 0
        finally:
            con.close()

        self.log.info("DuckDB SQL completed. Rows affected: %s", rows_changed)
        return {"rows": rows_changed}

    def _resolve_sql(self, context: Context) -> str:
        if self.sql_file:
            full_path = Path(SQL_PATH) / self.sql_file
            if not full_path.exists():
                raise FileNotFoundError(f"SQL file not found: {full_path}")
            raw_sql = full_path.read_text()
        elif self.sql:
            raw_sql = self.sql
        else:
            raise ValueError("Either sql_file or sql must be provided")

        # Render Jinja-style {{ params.X }} placeholders with template_params
        # and Airflow context (execution_date, ds, etc.)
        rendered = raw_sql
        all_params = {**self.template_params}

        # Add execution_date year as a common param
        if "execution_date" in context:
            all_params.setdefault("year", context["execution_date"].year)
            all_params.setdefault("ds", context["ds"])

        for key, value in all_params.items():
            rendered = rendered.replace(f"{{{{ params.{key} }}}}", str(value))
            rendered = rendered.replace(f"{{{{ {key} }}}}", str(value))

        return rendered
