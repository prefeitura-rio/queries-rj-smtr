{{config(
    materialized = 'table',
    partition_by = { 'field' :'data_versao',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['feed_publisher_name', 'data_versao'],
    alias = 'feed_info'
)}} 


SELECT SAFE_CAST(feed_publisher_name AS STRING) feed_publisher_name,
    SAFE_CAST(data AS DATE) data_versao,
    SAFE_CAST(JSON_VALUE(content, '$.feed_publisher_url') AS STRING) feed_publisher_url,
    SAFE_CAST(JSON_VALUE(content, '$.feed_lang') AS STRING) feed_lang,
    SAFE_CAST(JSON_VALUE(content, '$.feed_start_date') AS DATE) feed_start_date,
    SAFE_CAST(JSON_VALUE(content, '$.feed_end_date') AS DATE) feed_end_date,
    SAFE_CAST(JSON_VALUE(content, '$.feed_contact_email') AS STRING) feed_contact_email,
    
 FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'feed_info'
        ) }}
        
WHERE data_versao = '{{ var("data_versao_gtfs") }}'
