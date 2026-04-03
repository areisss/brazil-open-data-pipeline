-- Bronze: ingest raw IRPF CSV into partitioned Parquet.

COPY (
    SELECT
        year::INTEGER               AS year,
        income_bracket::VARCHAR     AS income_bracket,
        state::VARCHAR              AS state,
        num_declarations::BIGINT    AS num_declarations,
        taxable_income_brl::DOUBLE  AS taxable_income_brl,
        tax_due_brl::DOUBLE         AS tax_due_brl,
        current_timestamp           AS _ingested_at,
        '{{ params.source_file }}'  AS _source_file
    FROM read_csv_auto('{{ params.source_file }}', header=true)
)
TO '{{ params.bronze_path }}/irpf/' (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_CREATE);
