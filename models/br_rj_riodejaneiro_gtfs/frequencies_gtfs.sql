{{config(
    materialized = "incremental",
    partition_by = { "field" :"data",
    "data_type" :"date",
    "granularity": "day" },
    unique_key = ["trip_id", "data"],
    incremental_strategy = "insert_overwrite",
    alias = 'frequencies'
)}} 


SELECT SAFE_CAST(trip_id AS STRING) trip_id,
    SAFE_CAST(data AS DATE) data,
    SAFE_CAST(JSON_VALUE(content, "$.start_time") AS STRING) start_time,
    SAFE_CAST(JSON_VALUE(content, "$.end_time") AS STRING) end_time,
    SAFE_CAST(JSON_VALUE(content, "$.headway_secs") AS STRING) headway_secs,
    SAFE_CAST(JSON_VALUE(content, "$.exact_times") AS STRING) exact_times,
FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'frequencies'
        ) }}
WHERE data = "{{ var('data_versao_gtfs') }}"