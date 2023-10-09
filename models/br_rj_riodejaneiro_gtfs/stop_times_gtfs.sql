{{config(
    materialized = "incremental",
    partition_by = { "field" :"data",
    "data_type" :"date",
    "granularity": "day" },
    unique_key = ["trip_id", "data"],
    incremental_strategy = "insert_overwrite",
    alias = 'stop_times'
)}} 

WITH t AS (
    SELECT SAFE_CAST(trip_id AS STRING) trip_id,
        REPLACE(content, "None", "") content,
        SAFE_CAST(data AS DATE) data
    FROM {{ source('br_rj_riodejaneiro_gtfs_staging', 'stop_times') }}
)


SELECT trip_id,
    JSON_VALUE(content, "$.stop_sequence") stop_sequence,
    JSON_VALUE(content, "$.stop_id") stop_id,
    JSON_VALUE(content, "$.arrival_time") arrival_time,
    JSON_VALUE(content, "$.departure_time") departure_time,
    JSON_VALUE(content, "$.stop_headsign") stop_headsign,
    JSON_VALUE(content, "$.shape_dist_traveled") shape_dist_traveled,
    JSON_VALUE(content, "$.timepoint") timepoint,
    DATE(data) data
FROM t
