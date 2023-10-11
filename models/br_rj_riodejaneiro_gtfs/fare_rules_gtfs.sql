{{config(
    materialized = 'incremental',
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['fare_id', 'data'],
    incremental_strategy = 'insert_overwrite',
    alias = 'fare_rules'
)}} 

SELECT
    SAFE_CAST(data AS DATE) data,
    SAFE_CAST(JSON_VALUE(content, '$.fare_id') AS STRING) fare_id,
    SAFE_CAST(JSON_VALUE(content, '$.route_id') AS STRING) route_id,
    SAFE_CAST(JSON_VALUE(content, '$.agency_id') AS STRING) agency_id,
FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'fare_rules'
        ) }}
WHERE data = '{{ var("data_versao_gtfs") }}'
