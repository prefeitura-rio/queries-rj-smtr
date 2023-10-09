{ { config(
    materialized = "incremental",
    partition_by = { "field" :"data_versao_gtfs",
    "data_versao_gtfs_type" :"date",
    "granularity": "day" },
    unique_key = ["shape_id", "data_versao_gtfs"],
    incremental_strategy = "insert_overwrite",
    alias = 'shapes',
) } } 


WITH t AS (
    SELECT SAFE_CAST(shape_id AS STRING) shape_id,
        REPLACE(content, "None", "") content,
        SAFE_CAST(data_versao_gtfs AS DATE) data_versao_gtfs
    FROM { { source('br_rj_riodejaneiro_gtfs_staging', 'shapes') } }
)

SELECT shape_id,
    JSON_VALUE(content, "$.shape_pt_sequence") shape_pt_sequence,
    JSON_VALUE(content, "$.shape_pt_lat") shape_pt_lat,
    JSON_VALUE(content, "$.shape_pt_lon") shape_pt_lon,
    JSON_VALUE(content, "$.shape_dist_traveled") shape_dist_traveled,
    DATE(data_versao_gtfs) data_versao_gtfs
FROM t
