{{config(
    partition_by = { 'field' :'data_versao',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['route_id', 'data_versao'],
    alias = 'routes'
)}} 


SELECT SAFE_CAST(data_versao AS DATE) data_versao,
    SAFE_CAST(route_id AS STRING) route_id,
    SAFE_CAST(JSON_VALUE(content, '$.agency_id') AS STRING) agency_id,
    SAFE_CAST(JSON_VALUE(content, '$.route_short_name') AS STRING) route_short_name,
    SAFE_CAST(JSON_VALUE(content, '$.route_long_name') AS STRING) route_long_name,
    SAFE_CAST(JSON_VALUE(content, '$.route_desc') AS STRING) route_desc,
    SAFE_CAST(JSON_VALUE(content, '$.route_type') AS STRING) route_type,
    SAFE_CAST(JSON_VALUE(content, '$.route_url') AS STRING) route_url,
    SAFE_CAST(JSON_VALUE(content, '$.route_color') AS STRING) route_color,
    SAFE_CAST(JSON_VALUE(content, '$.route_text_color') AS STRING) route_text_color,
    SAFE_CAST(JSON_VALUE(content, '$.route_sort_order') AS INT64) route_sort_order,
    SAFE_CAST(JSON_VALUE(content, '$.continuous_pickup') AS STRING) continuous_pickup,
    SAFE_CAST(JSON_VALUE(content, '$.continuous_drop_off') AS STRING) continuous_drop_off,
    SAFE_CAST(JSON_VALUE(content, '$.network_id') AS STRING) network_id,
    '{{ var("version") }}' as versao_modelo
 FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'routes'
        ) }}
WHERE data_versao = '{{ var("data_versao_gtfs") }}'
