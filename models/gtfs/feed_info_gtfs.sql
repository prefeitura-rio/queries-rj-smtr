{{config(
  materialized='table',
  partition_by = { 'field' :'feed_start_date',
  'data_type' :'date',
  'granularity': 'day' },
  alias = 'feed_info'
)}} 


SELECT 
  SAFE_CAST(timestamp_captura AS STRING) AS feed_version, 
  SAFE_CAST(data_versao AS DATE) AS feed_start_date,
  DATE_SUB(LEAD(DATE(data_versao)) OVER (ORDER BY data_versao), INTERVAL 1 DAY) AS feed_end_date,
  SAFE_CAST(feed_publisher_name AS STRING) feed_publisher_name,
  SAFE_CAST(JSON_VALUE(content, '$.feed_publisher_url') AS STRING) feed_publisher_url,
  SAFE_CAST(JSON_VALUE(content, '$.feed_lang') AS STRING) feed_lang,
  SAFE_CAST(JSON_VALUE(content, '$.default_lang') AS STRING) default_lang,
  SAFE_CAST(JSON_VALUE(content, '$.feed_contact_email') AS STRING) feed_contact_email,
  SAFE_CAST(JSON_VALUE(content, '$.feed_contact_url') AS STRING) feed_contact_url,
  '{{ var("version") }}' AS versao_modelo
 FROM 
  {{ source(
    'br_rj_riodejaneiro_gtfs_staging',
    'feed_info'
  ) }}
