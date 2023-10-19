{{config(
    materialized = 'incremental',
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['route_id', 'data'],
    incremental_strategy = 'insert_overwrite',
    alias = 'routes'
)}} 

SELECT SAFE_CAST(route_id AS STRING) route_id,

    SAFE_CAST(data AS DATE) data,
    SAFE_CAST(JSON_VALUE(content, '$.agency_id') AS STRING) agency_id,
    SAFE_CAST(JSON_VALUE(content, '$.route_short_name') AS STRING) route_short_name,
    SAFE_CAST(JSON_VALUE(content, '$.route_long_name') AS STRING) route_long_name,
    SAFE_CAST(JSON_VALUE(content, '$.route_desc') AS STRING) route_desc,
    SAFE_CAST(JSON_VALUE(content, '$.route_type') AS STRING) route_type,
    SAFE_CAST(JSON_VALUE(content, '$.route_color') AS STRING) route_color,
    SAFE_CAST(JSON_VALUE(content, '$.route_text_color') AS STRING) route_text_color,
    
 FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'routes'
        ) }}
        
WHERE data = '{{ var("data_versao_gtfs") }}'
