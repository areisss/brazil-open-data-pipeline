-- Silver: clean and normalize PRODES deforestation data.
-- Standardizes biome and state names, filters invalid records.
-- Idempotent: full overwrite of silver/prodes/ partitions.

COPY (
    SELECT
        year,
        -- Normalize biome names
        CASE
            WHEN lower(biome) LIKE '%amazon%' OR lower(biome) LIKE '%amazônia%' THEN 'Amazonia'
            WHEN lower(biome) LIKE '%cerrado%' THEN 'Cerrado'
            WHEN lower(biome) LIKE '%atlantic%' OR lower(biome) LIKE '%mata%atlântica%' THEN 'Mata Atlantica'
            WHEN lower(biome) LIKE '%caatinga%' THEN 'Caatinga'
            WHEN lower(biome) LIKE '%pampa%' THEN 'Pampa'
            WHEN lower(biome) LIKE '%pantanal%' THEN 'Pantanal'
            ELSE biome
        END AS biome,
        -- Normalize state codes to uppercase 2-letter
        CASE
            WHEN length(state) = 2 THEN upper(state)
            WHEN upper(state) = 'ALL' THEN 'ALL'
            ELSE upper(state)
        END AS state,
        round(area_km2, 2) AS area_km2,
        current_timestamp AS _processed_at
    FROM read_parquet('{{ params.bronze_path }}/prodes/**/*.parquet')
    WHERE year BETWEEN 2000 AND 2025
      AND area_km2 > 0
      AND area_km2 < 100000  -- sanity check: no single entry should exceed 100k km²
)
TO '{{ params.silver_path }}/prodes/' (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE);
