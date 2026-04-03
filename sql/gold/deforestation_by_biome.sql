-- Gold: annual deforestation by biome.
-- Aggregates total area deforested per biome per year.
-- Adds cumulative totals and YoY change.

COPY (
    WITH annual AS (
        SELECT
            year,
            biome,
            sum(area_km2) AS area_km2
        FROM read_parquet('{{ params.silver_path }}/prodes/**/*.parquet')
        WHERE state = 'ALL' OR state NOT IN (
            -- Avoid double counting: if 'ALL' row exists for a biome+year,
            -- don't also sum state-level rows
            SELECT DISTINCT state
            FROM read_parquet('{{ params.silver_path }}/prodes/**/*.parquet') sub
            WHERE sub.state = 'ALL' AND sub.year = year AND sub.biome = biome
        )
        GROUP BY year, biome
    ),
    with_cumulative AS (
        SELECT
            year,
            biome,
            area_km2,
            sum(area_km2) OVER (PARTITION BY biome ORDER BY year) AS cumulative_km2,
            lag(area_km2) OVER (PARTITION BY biome ORDER BY year) AS prev_year_km2
        FROM annual
    )
    SELECT
        year,
        biome,
        round(area_km2, 2) AS area_km2,
        round(cumulative_km2, 2) AS cumulative_km2,
        round(prev_year_km2, 2) AS prev_year_km2,
        CASE
            WHEN prev_year_km2 > 0
            THEN round((area_km2 - prev_year_km2) / prev_year_km2 * 100, 1)
            ELSE NULL
        END AS yoy_change_pct,
        current_timestamp AS _computed_at
    FROM with_cumulative
    ORDER BY biome, year
)
TO '{{ params.gold_path }}/deforestation_by_biome/' (FORMAT PARQUET, PARTITION_BY (biome), OVERWRITE_OR_CREATE);
