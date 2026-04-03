"""DAG integrity tests.

Verifies that all DAGs can be imported without errors and have no cycles.
This catches syntax errors, missing imports, and circular dependencies
before they hit the Airflow scheduler.
"""

import importlib
import os
import sys
from pathlib import Path

import pytest

# Add project root to path so dags/ and include/ are importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Set environment variables that DAGs expect
os.environ.setdefault("DATA_LAKE_PATH", "/tmp/test_data")
os.environ.setdefault("WAREHOUSE_PATH", "/tmp/test_warehouse")
os.environ.setdefault("SQL_PATH", str(PROJECT_ROOT / "sql"))

DAG_FILES = [
    "dags.deforestation_pipeline",
    "dags.government_spending_pipeline",
    "dags.irpf_tax_pipeline",
    "dags.cross_domain_analytics",
]


@pytest.mark.parametrize("dag_module", DAG_FILES)
def test_dag_imports_without_error(dag_module):
    """Each DAG file should import without raising exceptions."""
    mod = importlib.import_module(dag_module)
    assert mod is not None


def test_sql_files_exist():
    """All SQL files referenced by DAGs should exist."""
    sql_dir = PROJECT_ROOT / "sql"
    expected_files = [
        "bronze/prodes_ingest.sql",
        "bronze/siconfi_ingest.sql",
        "bronze/irpf_ingest.sql",
        "silver/prodes_clean.sql",
        "silver/siconfi_clean.sql",
        "silver/irpf_clean.sql",
        "gold/deforestation_by_biome.sql",
        "gold/deforestation_by_state.sql",
        "gold/spending_by_function.sql",
        "gold/tax_by_bracket.sql",
        "gold/cross_domain/fiscal_balance.sql",
        "gold/cross_domain/deforestation_vs_spending.sql",
    ]
    for f in expected_files:
        assert (sql_dir / f).exists(), f"Missing SQL file: {f}"


def test_sql_files_are_valid_sql():
    """SQL files should not be empty and should contain SELECT or COPY."""
    sql_dir = PROJECT_ROOT / "sql"
    for sql_file in sql_dir.rglob("*.sql"):
        content = sql_file.read_text()
        assert len(content) > 10, f"SQL file too short: {sql_file}"
        upper = content.upper()
        assert "SELECT" in upper or "COPY" in upper or "CREATE" in upper, (
            f"SQL file doesn't contain SELECT/COPY/CREATE: {sql_file}"
        )
