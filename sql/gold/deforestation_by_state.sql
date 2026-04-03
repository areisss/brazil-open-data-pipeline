-- Gold: annual deforestation by state.
-- Shows which states are the top deforesters and how they trend over time.

COPY (
    WITH state_data AS (
        SELECT
            year,
            biome,
            state,
            sum(area_km2) AS area_km2
        FROM read_parquet('{{ params.silver_path }}/prodes/**/*.parquet')
        WHERE state != 'ALL'
        GROUP BY year, biome, state
    ),
    ranked AS (
        SELECT
            year,
            biome,
            state,
            area_km2,
            lag(area_km2) OVER (PARTITION BY biome, state ORDER BY year) AS prev_year_km2,
            rank() OVER (PARTITION BY year, biome ORDER BY area_km2 DESC) AS rank_in_year
        FROM state_data
    )
    SELECT
        year,
        biome,
        state,
        round(area_km2, 2) AS area_km2,
        round(prev_year_km2, 2) AS prev_year_km2,
        CASE
            WHEN prev_year_km2 > 0
            THEN round((area_km2 - prev_year_km2) / prev_year_km2 * 100, 1)
            ELSE NULL
        END AS yoy_change_pct,
        rank_in_year,
        current_timestamp AS _computed_at
    FROM ranked
    ORDER BY year DESC, biome, area_km2 DESC
)
TO '{{ params.gold_path }}/deforestation_by_state/data.parquet' (FORMAT PARQUET);
