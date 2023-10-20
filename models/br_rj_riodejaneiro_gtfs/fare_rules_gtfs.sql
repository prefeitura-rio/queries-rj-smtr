{{config(
    partition_by = { 'field' :'data_versao',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['fare_id', 'data_versao'],
    alias = 'fare_rules'
)}} 


SELECT
    SAFE_CAST(data_versao AS DATE) data_versao,
    SAFE_CAST(JSON_VALUE(content, '$.fare_id') AS STRING) fare_id,
    SAFE_CAST(JSON_VALUE(content, '$.route_id') AS STRING) route_id,
    SAFE_CAST(JSON_VALUE(content, '$.origin_id') AS STRING) origin_id,
    SAFE_CAST(JSON_VALUE(content, '$.destination_id') AS STRING) destination_id,
    SAFE_CAST(JSON_VALUE(content, '$.contains_id') AS STRING) contains_id,
    '{{ var("version") }}' as versao_modelo
 FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'fare_rules'
        ) }}
WHERE data_versao = '{{ var("data_versao_gtfs") }}'
