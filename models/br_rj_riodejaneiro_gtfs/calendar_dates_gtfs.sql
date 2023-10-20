{{ config(
    partition_by = { 'field' :'data_versao',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['service_id', 'date', 'data_versao'],
    alias = 'calendar_dates'
) }}


SELECT SAFE_CAST(data_versao AS DATE) data_versao,
        SAFE_CAST(service_id AS STRING) service_id,
        PARSE_DATE('%Y%m%d', SAFE_CAST(date AS STRING)) date,
        SAFE_CAST(JSON_VALUE(content, '$.exception_type') AS STRING) exception_type,
        '{{ var("version") }}' as versao_modelo
FROM {{ source(
        'br_rj_riodejaneiro_gtfs_staging',
        'calendar_dates'
    ) }}
WHERE data_versao = '{{ var("data_versao_gtfs") }}'
