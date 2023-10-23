{{config( 
    partition_by = { 'field' :'data_versao',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['trip_id','data_versao'],
    alias = 'trips' 
)}}


SELECT SAFE_CAST(data_versao AS DATE) data_versao,
    SAFE_CAST(JSON_VALUE(content, '$.route_id') AS STRING) route_id,
    SAFE_CAST(JSON_VALUE(content, '$.service_id') AS STRING) service_id,
    SAFE_CAST(trip_id AS STRING) trip_id,
    SAFE_CAST(JSON_VALUE(content, '$.trip_headsign') AS STRING) trip_headsign,
    SAFE_CAST(JSON_VALUE(content, '$.trip_short_name') AS STRING) trip_short_name,
    SAFE_CAST(JSON_VALUE(content, '$.direction_id') AS STRING) direction_id,
    SAFE_CAST(JSON_VALUE(content, '$.block_id') AS STRING) block_id,
    SAFE_CAST(JSON_VALUE(content, '$.shape_id') AS STRING) shape_id,
    SAFE_CAST(JSON_VALUE(content, '$.wheelchair_accessible') AS STRING) wheelchair_accessible,
    SAFE_CAST(JSON_VALUE(content, '$.bikes_allowed') AS STRING) bikes_allowed,
    '{{ var("version") }}' as versao_modelo
FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'trips'
        ) }}
WHERE data_versao = '{{ var("data_versao_gtfs") }}'
