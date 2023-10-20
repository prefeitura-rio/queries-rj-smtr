{{config(
    partition_by = { 'field' :'data_versao',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['fare_id', 'data_versao'],
    alias = 'fare_attributes'
)}} 


SELECT SAFE_CAST(data_versao AS DATE) data_versao,
        SAFE_CAST(fare_id AS STRING) fare_id,
        SAFE_CAST(JSON_VALUE(content, '$.price') AS FLOAT64) price,
        SAFE_CAST(JSON_VALUE(content, '$.currency_type') AS STRING) currency_type,
        SAFE_CAST(JSON_VALUE(content, '$.payment_method') AS STRING) payment_method,
        SAFE_CAST(JSON_VALUE(content, '$.transfers') AS STRING) transfers,
        SAFE_CAST(JSON_VALUE(content, '$.agency_id') AS STRING) agency_id,
        SAFE_CAST(JSON_VALUE(content, '$.transfer_duration') AS INT64) transfer_duration,
        '{{ var("version") }}' as versao_modelo
FROM {{source(
            'br_rj_riodejaneiro_gtfs_staging',
            'fare_attributes'
        )}}
WHERE data_versao = '{{ var("data_versao_gtfs") }}'
