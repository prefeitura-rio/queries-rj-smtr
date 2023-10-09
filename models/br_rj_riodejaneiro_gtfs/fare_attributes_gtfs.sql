{{config(
    materialized = "incremental",
    partition_by = { "field" :"data",
    "data_type" :"date",
    "granularity": "day" },
    unique_key = ["fare_id", "data"],
    incremental_strategy = "insert_overwrite",
    alias = 'fare_attributes'
)}} 


SELECT SAFE_CAST(fare_id AS STRING) fare_id,
    SAFE_CAST(data AS DATE) data
    SAFE_CAST(JSON_VALUE(content, "$.price") AS STRING) price,
    SAFE_CAST(JSON_VALUE(content, "$.currency_type") AS STRING) currency_type,
    SAFE_CAST(JSON_VALUE(content, "$.payment_method") AS STRING) payment_method,
    SAFE_CAST(SON_VALUE(content, "$.transfers") AS STRING) transfers,
    SAFE_CAST(JSON_VALUE(content, "$.agency_id") AS STRING) agency_id,
    SAFE_CAST(JSON_VALUE(content, "$.transfer_duration") AS STRING) transfer_duration,
FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'fare_attributes'
        ) }}
WHERE data = "{{ var('data_versao_gtfs') }}"