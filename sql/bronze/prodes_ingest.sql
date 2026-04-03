-- Bronze: ingest raw PRODES CSV into partitioned Parquet.
-- Preserves raw data as-is, adds ingestion metadata.
-- Idempotent: overwrites the partition for the given year.

COPY (
    SELECT
        year::INTEGER       AS year,
        biome::VARCHAR      AS biome,
        state::VARCHAR      AS state,
        area_km2::DOUBLE    AS area_km2,
        current_timestamp   AS _ingested_at,
        '{{ params.source_file }}' AS _source_file
    FROM read_csv_auto('{{ params.source_file }}', header=true)
)
TO '{{ params.bronze_path }}/prodes/' (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_CREATE);
