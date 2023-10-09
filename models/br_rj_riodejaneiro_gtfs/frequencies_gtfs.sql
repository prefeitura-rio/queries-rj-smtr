{ { config(
    materialized = "incremental",
    partition_by = { "field" :"data_versao_gtfs",
    "data_versao_gtfs_type" :"date",
    "granularity": "day" },
    unique_key = ["trip_id", "data_versao_gtfs"],
    incremental_strategy = "insert_overwrite",
    alias = 'frequencies',
) } } 

WITH t AS (
    SELECT SAFE_CAST(trip_id AS STRING) trip_id,
        REPLACE(content, "None", "") content,
        SAFE_CAST(data_versao_gtfs AS DATE) data_versao_gtfs
    FROM { { source('br_rj_riodejaneiro_gtfs_staging', 'frequencies') } }
)

SELECT trip_id,
    JSON_VALUE(content, "$.start_time") start_time,
    JSON_VALUE(content, "$.end_time") end_time,
    JSON_VALUE(content, "$.headway_secs") headway_secs,
    JSON_VALUE(content, "$.exact_times") exact_times,
    DATE(data_versao_gtfs) data_versao_gtfs
FROM t
