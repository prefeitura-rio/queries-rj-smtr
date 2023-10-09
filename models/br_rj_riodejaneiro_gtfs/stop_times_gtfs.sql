{{config(
    materialized = "incremental",
    partition_by = { "field" :"data",
    "data_type" :"date",
    "granularity": "day" },
    unique_key = ["trip_id", "data"],
    incremental_strategy = "insert_overwrite",
    alias = 'stop_times'
)}} 

SELECT SAFE_CAST(trip_id AS STRING) trip_id,
    SAFE_CAST(data AS DATE) data,
    SAFE_CAST(JSON_VALUE(content, "$.stop_sequence") AS STRING) stop_sequence,
    SAFE_CAST(JSON_VALUE(content, "$.stop_id") AS STRING) stop_id,
    SAFE_CAST(JSON_VALUE(content, "$.arrival_time") AS STRING) arrival_time,
    SAFE_CAST(JSON_VALUE(content, "$.departure_time") AS STRING) departure_time,
    SAFE_CAST(JSON_VALUE(content, "$.stop_headsign") AS STRING) stop_headsign,
    SAFE_CAST(JSON_VALUE(content, "$.shape_dist_traveled") AS STRING) shape_dist_traveled,
    SAFE_CAST(JSON_VALUE(content, "$.timepoint") AS STRING) timepoint,
FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'stop_times'
        ) }}
WHERE data = "{{ var('data_versao_gtfs') }}"
