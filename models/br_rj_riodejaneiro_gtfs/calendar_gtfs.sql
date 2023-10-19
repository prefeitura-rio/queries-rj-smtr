{{config(
    materialized = 'incremental',
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['service_id', 'data'],
    incremental_strategy = 'insert_overwrite',
    alias = 'calendar'
)}} 


SELECT SAFE_CAST(service_id AS STRING) service_id,
    SAFE_CAST(data AS DATE) data,
    SAFE_CAST(JSON_VALUE(content, '$.monday') AS STRING) monday,
    SAFE_CAST(JSON_VALUE(content, '$.tuesday') AS STRING) tuesday,
    SAFE_CAST(JSON_VALUE(content, '$.wednesday') AS STRING) wednesday,
    SAFE_CAST(JSON_VALUE(content, '$.thursday') AS STRING) thursday,
    SAFE_CAST(JSON_VALUE(content, '$.friday') AS STRING) friday,
    SAFE_CAST(JSON_VALUE(content, '$.saturday') AS STRING) saturday,
    SAFE_CAST(JSON_VALUE(content, '$.sunday') AS STRING) sunday,
    SAFE_CAST(JSON_VALUE(content, '$.start_date') AS STRING) start_date,
    SAFE_CAST(JSON_VALUE(content, '$.end_date') AS STRING) end_date,
    
 FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'calendar'
        ) }}
        
WHERE data = '{{ var("data_versao_gtfs") }}'
