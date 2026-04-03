-- Gold: federal spending by functional area over time.
-- Shows how government priorities shift across health, education, defense, etc.

COPY (
    WITH annual AS (
        SELECT
            year,
            functional_category,
            sum(executed_brl) AS executed_brl,
            sum(budgeted_brl) AS budgeted_brl
        FROM read_parquet('{{ params.silver_path }}/spending/**/*.parquet')
        GROUP BY year, functional_category
    ),
    totals AS (
        SELECT year, sum(executed_brl) AS total_executed
        FROM annual
        GROUP BY year
    ),
    enriched AS (
        SELECT
            a.year,
            a.functional_category,
            a.executed_brl,
            a.budgeted_brl,
            CASE WHEN t.total_executed > 0
                THEN round(a.executed_brl / t.total_executed * 100, 2)
                ELSE 0
            END AS pct_of_total,
            lag(a.executed_brl) OVER (PARTITION BY a.functional_category ORDER BY a.year) AS prev_year_brl
        FROM annual a
        JOIN totals t ON a.year = t.year
    )
    SELECT
        year,
        functional_category,
        round(executed_brl, 2) AS executed_brl,
        round(budgeted_brl, 2) AS budgeted_brl,
        pct_of_total,
        CASE WHEN prev_year_brl > 0
            THEN round((executed_brl - prev_year_brl) / prev_year_brl * 100, 1)
            ELSE NULL
        END AS yoy_nominal_growth_pct,
        current_timestamp AS _computed_at
    FROM enriched
    ORDER BY year, executed_brl DESC
)
TO '{{ params.gold_path }}/spending_by_function/' (FORMAT PARQUET, PARTITION_BY (functional_category), OVERWRITE);
