-- Cross-domain: Deforestation vs environmental spending.
-- Does more spending on environment correlate with less deforestation?

COPY (
    WITH deforestation AS (
        SELECT year, sum(area_km2) AS total_deforested_km2
        FROM read_parquet('{{ params.gold_path }}/deforestation_by_biome/**/*.parquet')
        GROUP BY year
    ),
    env_spending AS (
        SELECT year, sum(executed_brl) AS env_spending_brl
        FROM read_parquet('{{ params.gold_path }}/spending_by_function/**/*.parquet')
        WHERE functional_category = 'Environment'
        GROUP BY year
    )
    SELECT
        coalesce(d.year, e.year) AS year,
        round(coalesce(d.total_deforested_km2, 0), 2) AS deforested_km2,
        round(coalesce(e.env_spending_brl, 0), 2) AS env_spending_brl,
        CASE WHEN d.total_deforested_km2 > 0
            THEN round(e.env_spending_brl / d.total_deforested_km2, 0)
            ELSE NULL
        END AS spending_per_km2_deforested,
        current_timestamp AS _computed_at
    FROM deforestation d
    FULL OUTER JOIN env_spending e ON d.year = e.year
    ORDER BY year
)
TO '{{ params.gold_path }}/cross_domain/deforestation_vs_spending/' (FORMAT PARQUET, OVERWRITE_OR_CREATE);
