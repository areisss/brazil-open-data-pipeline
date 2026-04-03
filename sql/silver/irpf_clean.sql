-- Silver: clean and normalize IRPF tax data.

COPY (
    SELECT
        year,
        income_bracket,
        upper(state) AS state,
        num_declarations,
        round(taxable_income_brl, 2) AS taxable_income_brl,
        round(tax_due_brl, 2) AS tax_due_brl,
        CASE WHEN taxable_income_brl > 0
            THEN round(tax_due_brl / taxable_income_brl * 100, 2)
            ELSE 0
        END AS effective_tax_rate_pct,
        current_timestamp AS _processed_at
    FROM read_parquet('{{ params.bronze_path }}/irpf/**/*.parquet')
    WHERE year BETWEEN 2008 AND 2025
      AND num_declarations >= 0
      AND tax_due_brl >= 0
)
TO '{{ params.silver_path }}/irpf/' (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE);
