{{config(
    materialized = "incremental",
    partition_by = { "field" :"data",
    "data_type" :"date",
    "granularity": "day" },
    unique_key = ["fare_id", "data"],
    incremental_strategy = "insert_overwrite",
    alias = 'fare_attributes'
)}} 

WITH t AS (
    SELECT SAFE_CAST(fare_id AS STRING) fare_id,
        REPLACE(content, "None", "") content,
        SAFE_CAST(data AS DATE) data
    FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'fare_attributes'
        ) }}
)

SELECT fare_id,
    JSON_VALUE(content, "$.price") price,
    JSON_VALUE(content, "$.currency_type") currency_type,
    JSON_VALUE(content, "$.payment_method") payment_method,
    JSON_VALUE(content, "$.transfers") transfers,
    JSON_VALUE(content, "$.agency_id") agency_id,
    JSON_VALUE(content, "$.transfer_duration") transfer_duration,
    DATE(data) data
FROM t
