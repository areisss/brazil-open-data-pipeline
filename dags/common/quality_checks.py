"""Data quality checks executed via DuckDB SQL.

Each check runs a SQL query against the warehouse and asserts the result meets
expectations. Checks are composable — DAGs pass a list of checks to run.
"""

import os

import duckdb

from dags.common.constants import DUCKDB_PATH


class QualityCheckFailed(Exception):
    """Raised when a data quality check fails."""


def run_quality_checks(checks: list[dict], **kwargs) -> dict:
    """Execute a list of quality checks against DuckDB.

    Each check is a dict:
        {
            "name": "row_count_bronze_prodes",
            "sql": "SELECT count(*) FROM read_parquet('data/bronze/prodes/**/*.parquet')",
            "op": "gt",        # gt, gte, lt, lte, eq, between
            "threshold": 0,    # single value or [min, max] for "between"
        }

    Returns a summary dict with pass/fail counts.
    """
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)

    results = {"passed": 0, "failed": 0, "details": []}

    for check in checks:
        name = check["name"]
        sql = check["sql"]
        op = check.get("op", "gt")
        threshold = check.get("threshold", 0)

        try:
            value = con.execute(sql).fetchone()[0]
            passed = _evaluate(value, op, threshold)

            results["details"].append({
                "name": name,
                "value": value,
                "threshold": threshold,
                "op": op,
                "passed": passed,
            })

            if passed:
                results["passed"] += 1
            else:
                results["failed"] += 1

        except Exception as exc:
            results["failed"] += 1
            results["details"].append({
                "name": name,
                "error": str(exc),
                "passed": False,
            })

    con.close()

    if results["failed"] > 0:
        failed_names = [d["name"] for d in results["details"] if not d["passed"]]
        raise QualityCheckFailed(
            f"{results['failed']} quality check(s) failed: {', '.join(failed_names)}. "
            f"Details: {results['details']}"
        )

    return results


def _evaluate(value, op: str, threshold) -> bool:
    if op == "gt":
        return value > threshold
    if op == "gte":
        return value >= threshold
    if op == "lt":
        return value < threshold
    if op == "lte":
        return value <= threshold
    if op == "eq":
        return value == threshold
    if op == "between":
        return threshold[0] <= value <= threshold[1]
    if op == "not_null":
        return value is not None and value > 0
    raise ValueError(f"Unknown operator: {op}")
