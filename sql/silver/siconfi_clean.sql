-- Silver: clean and normalize government spending data.
-- Standardizes functional categories, removes invalid records.

COPY (
    SELECT
        year,
        -- Normalize functional category names
        CASE
            WHEN lower(functional_category) LIKE '%saude%' OR lower(functional_category) LIKE '%saúde%' THEN 'Health'
            WHEN lower(functional_category) LIKE '%educa%' THEN 'Education'
            WHEN lower(functional_category) LIKE '%previd%' THEN 'Social Security'
            WHEN lower(functional_category) LIKE '%defesa%' OR lower(functional_category) LIKE '%defense%' THEN 'Defense'
            WHEN lower(functional_category) LIKE '%assist%social%' THEN 'Social Assistance'
            WHEN lower(functional_category) LIKE '%meio%ambient%' OR lower(functional_category) LIKE '%environment%' THEN 'Environment'
            WHEN lower(functional_category) LIKE '%ci%ncia%' OR lower(functional_category) LIKE '%tecnol%' OR lower(functional_category) LIKE '%science%' THEN 'Science & Technology'
            WHEN lower(functional_category) LIKE '%seguran%' OR lower(functional_category) LIKE '%security%' THEN 'Public Safety'
            ELSE functional_category
        END AS functional_category,
        description,
        round(budgeted_brl, 2) AS budgeted_brl,
        round(executed_brl, 2) AS executed_brl,
        CASE
            WHEN budgeted_brl > 0 THEN round(executed_brl / budgeted_brl * 100, 1)
            ELSE 0
        END AS execution_rate_pct,
        current_timestamp AS _processed_at
    FROM read_parquet('{{ params.bronze_path }}/spending/**/*.parquet')
    WHERE year BETWEEN 2000 AND 2025
      AND executed_brl >= 0
)
TO '{{ params.silver_path }}/spending/' (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_CREATE);
