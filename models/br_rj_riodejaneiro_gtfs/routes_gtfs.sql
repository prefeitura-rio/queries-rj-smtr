{ { config(
    materialized = "incremental",
    partition_by = { "field" :"data_versao_gtfs",
    "data_versao_gtfs_type" :"date",
    "granularity": "day" },
    unique_key = ["route_id", "data_versao_gtfs"],
    incremental_strategy = "insert_overwrite",
    alias = 'routes',
) } } 

WITH t AS (
    SELECT SAFE_CAST(route_id AS STRING) route_id,
        REPLACE(content, "None", "") content,
        SAFE_CAST(data_versao_gtfs AS DATE) data_versao_gtfs
    FROM { { source('br_rj_riodejaneiro_gtfs_staging', 'routes') } }
)

SELECT route_id,
    JSON_VALUE(content, "$.agency_id") agency_id,
    JSON_VALUE(content, "$.route_short_name") route_short_name,
    JSON_VALUE(content, "$.route_long_name") route_long_name,
    JSON_VALUE(content, "$.route_desc") route_desc,
    JSON_VALUE(content, "$.route_type") route_type,
    JSON_VALUE(content, "$.route_color") route_color,
    JSON_VALUE(content, "$.route_text_color") route_text_color,
    DATE(data_versao_gtfs) data_versao_gtfs
FROM t
