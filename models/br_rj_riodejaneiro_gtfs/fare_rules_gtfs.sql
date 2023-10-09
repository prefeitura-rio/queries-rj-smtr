{{config(
    materialized = "incremental",
    partition_by = { "field" :"data",
    "data_type" :"date",
    "granularity": "day" },
    unique_key = ["fare_id", "data"],
    incremental_strategy = "insert_overwrite",
    alias = 'fare_rules'
)}} 

WITH t AS (
    SELECT SAFE_CAST(fare_id AS STRING) fare_id,
        REPLACE(content, "None", "") content,
        SAFE_CAST(data AS DATE) data
    FROM {{ source('br_rj_riodejaneiro_gtfs_staging', 'fare_rules') }}
)
SELECT fare_id,
    JSON_VALUE(content, "$.route_id") route_id,
    JSON_VALUE(content, "$.agency_id") agency_id,
    DATE(data) data
FROM t
