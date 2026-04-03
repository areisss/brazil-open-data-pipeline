-- Cross-domain: Tax revenue vs government spending over time.
-- Shows the fiscal balance trend — are we collecting enough to fund spending?

COPY (
    WITH tax_annual AS (
        SELECT year, sum(total_tax_due) AS total_tax_revenue
        FROM read_parquet('{{ params.gold_path }}/tax_by_bracket/**/*.parquet')
        GROUP BY year
    ),
    spending_annual AS (
        SELECT year, sum(executed_brl) AS total_spending
        FROM read_parquet('{{ params.gold_path }}/spending_by_function/**/*.parquet')
        GROUP BY year
    )
    SELECT
        coalesce(t.year, s.year) AS year,
        round(coalesce(t.total_tax_revenue, 0), 2) AS irpf_revenue_brl,
        round(coalesce(s.total_spending, 0), 2) AS total_spending_brl,
        round(coalesce(t.total_tax_revenue, 0) - coalesce(s.total_spending, 0), 2) AS balance_brl,
        CASE WHEN s.total_spending > 0
            THEN round(t.total_tax_revenue / s.total_spending * 100, 1)
            ELSE NULL
        END AS irpf_covers_pct,
        current_timestamp AS _computed_at
    FROM tax_annual t
    FULL OUTER JOIN spending_annual s ON t.year = s.year
    ORDER BY year
)
TO '{{ params.gold_path }}/cross_domain/fiscal_balance/' (FORMAT PARQUET, OVERWRITE_OR_CREATE);
