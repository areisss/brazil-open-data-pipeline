-- Bronze: ingest raw SICONFI spending CSV into partitioned Parquet.

COPY (
    SELECT
        year::INTEGER                   AS year,
        functional_category::VARCHAR    AS functional_category,
        description::VARCHAR            AS description,
        budgeted_brl::DOUBLE            AS budgeted_brl,
        executed_brl::DOUBLE            AS executed_brl,
        current_timestamp               AS _ingested_at,
        '{{ params.source_file }}'      AS _source_file
    FROM read_csv_auto('{{ params.source_file }}', header=true)
)
TO '{{ params.bronze_path }}/spending/' (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_CREATE);
