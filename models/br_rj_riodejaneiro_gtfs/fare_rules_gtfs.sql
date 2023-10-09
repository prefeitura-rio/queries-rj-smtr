{ { config(
    materialized = "incremental",
    partition_by = { "field" :"data_versao_gtfs",
    "data_versao_gtfs_type" :"date",
    "granularity": "day" },
    unique_key = ["fare_id", "data_versao_gtfs"],
    incremental_strategy = "insert_overwrite",
    alias = 'fare_rules',
) } } 

WITH t AS (
    SELECT SAFE_CAST(fare_id AS STRING) fare_id,
        REPLACE(content, "None", "") content,
        SAFE_CAST(data_versao_gtfs AS DATE) data_versao_gtfs
    FROM { { source('br_rj_riodejaneiro_gtfs_staging', 'fare_rules') } }
)
SELECT fare_id,
    JSON_VALUE(content, "$.route_id") route_id,
    JSON_VALUE(content, "$.agency_id") agency_id,
    DATE(data_versao_gtfs) data_versao_gtfs
FROM t
