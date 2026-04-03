-- Quality checks for PRODES deforestation data.
-- Each query returns a single value that is compared against a threshold.

-- Check 1: Bronze row count > 0
-- name: bronze_row_count
SELECT count(*) FROM read_parquet('{{ params.bronze_path }}/prodes/**/*.parquet');

-- Check 2: Silver row count > 0
-- name: silver_row_count
SELECT count(*) FROM read_parquet('{{ params.silver_path }}/prodes/**/*.parquet');

-- Check 3: No null years in silver
-- name: silver_null_years
SELECT count(*) FROM read_parquet('{{ params.silver_path }}/prodes/**/*.parquet') WHERE year IS NULL;

-- Check 4: No negative areas in silver
-- name: silver_negative_areas
SELECT count(*) FROM read_parquet('{{ params.silver_path }}/prodes/**/*.parquet') WHERE area_km2 < 0;

-- Check 5: Gold deforestation_by_biome has data
-- name: gold_biome_row_count
SELECT count(*) FROM read_parquet('{{ params.gold_path }}/deforestation_by_biome/**/*.parquet');
