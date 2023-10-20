{{ config(
    materialized = 'table',
    partition_by = { 'field' :'data_versao',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['service_id', 'date', 'data_versao'],
    alias = 'calendar_dates'
) }}

SELECT SAFE_CAST(service_id AS STRING) service_id,
    SAFE_CAST (date AS DATE) data_versao,
    SAFE_CAST(data AS DATE) data_versao,
    SAFE_CAST(JSON_VALUE(content, '$.monday') AS INT64) monday,
    SAFE_CAST(JSON_VALUE(content, '$.tuesday') AS INT64) tuesday,
    SAFE_CAST(JSON_VALUE(content, '$.wednesday') AS INT64) wednesday,
    SAFE_CAST(JSON_VALUE(content, '$.thursday') AS INT64) thursday,
    SAFE_CAST(JSON_VALUE(content, '$.friday') AS INT64) friday,
    SAFE_CAST(JSON_VALUE(content, '$.saturday') AS INT64) saturday,
    SAFE_CAST(JSON_VALUE(content, '$.sunday') AS INT64) sunday,
    SAFE_CAST(JSON_VALUE(content, '$.start_date') AS DATE) start_date,
    SAFE_CAST(JSON_VALUE(content, '$.end_date') AS DATE) end_date,
    
FROM {{ source(
        'br_rj_riodejaneiro_gtfs_staging',
        'calendar_dates'
    ) }}
    
WHERE data_versao = '{{ var("data_versao_gtfs") }}'
