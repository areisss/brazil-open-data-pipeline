-- Gold: tax revenue and declarations by income bracket over time.
-- Shows who pays how much tax and how the burden shifts.

COPY (
    WITH bracket_annual AS (
        SELECT
            year,
            income_bracket,
            sum(num_declarations) AS total_declarations,
            sum(taxable_income_brl) AS total_taxable_income,
            sum(tax_due_brl) AS total_tax_due,
            CASE WHEN sum(taxable_income_brl) > 0
                THEN round(sum(tax_due_brl) / sum(taxable_income_brl) * 100, 2)
                ELSE 0
            END AS effective_rate_pct
        FROM read_parquet('{{ params.silver_path }}/irpf/**/*.parquet')
        WHERE state = 'ALL'
        GROUP BY year, income_bracket
    ),
    year_totals AS (
        SELECT year, sum(total_tax_due) AS grand_total_tax
        FROM bracket_annual
        GROUP BY year
    ),
    enriched AS (
        SELECT
            b.year,
            b.income_bracket,
            b.total_declarations,
            b.total_taxable_income,
            b.total_tax_due,
            b.effective_rate_pct,
            CASE WHEN y.grand_total_tax > 0
                THEN round(b.total_tax_due / y.grand_total_tax * 100, 2)
                ELSE 0
            END AS pct_of_total_tax,
            lag(b.total_tax_due) OVER (PARTITION BY b.income_bracket ORDER BY b.year) AS prev_tax
        FROM bracket_annual b
        JOIN year_totals y ON b.year = y.year
    )
    SELECT
        year,
        income_bracket,
        total_declarations,
        round(total_taxable_income, 2) AS total_taxable_income,
        round(total_tax_due, 2) AS total_tax_due,
        effective_rate_pct,
        pct_of_total_tax,
        CASE WHEN prev_tax > 0
            THEN round((total_tax_due - prev_tax) / prev_tax * 100, 1)
            ELSE NULL
        END AS yoy_growth_pct,
        current_timestamp AS _computed_at
    FROM enriched
    ORDER BY year, income_bracket
)
TO '{{ params.gold_path }}/tax_by_bracket/' (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_CREATE);
