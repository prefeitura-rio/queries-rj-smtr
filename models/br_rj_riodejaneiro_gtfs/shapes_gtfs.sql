{{ config(
    materialized = "incremental",
    partition_by = { "field" :"data",
    "data_type" :"date",
    "granularity": "day" },
    unique_key = ["shape_id", "data"],
    incremental_strategy = "insert_overwrite",
    alias = 'shapes'
) }}
SELECT SAFE_CAST(shape_id AS STRING) shape_id,
    SAFE_CAST(data AS DATE) data, 
    SAFE_CAST(JSON_VALUE(content, "$.shape_pt_sequence") AS STRING) shape_pt_sequence,
    SAFE_CAST(JSON_VALUE(content, "$.shape_pt_lat") AS FLOAT64) shape_pt_lat,
    SAFE_CAST(JSON_VALUE(content, "$.shape_pt_lon") AS FLOAT64) shape_pt_lon,
    SAFE_CAST(
        JSON_VALUE(content, "$.shape_dist_traveled") AS STRING
    ) shape_dist_traveled,
    FROM {{ source(
        'br_rj_riodejaneiro_gtfs_staging',
        'shapes'
    ) }}
WHERE data = "{{ var('data_versao_gtfs') }}"
