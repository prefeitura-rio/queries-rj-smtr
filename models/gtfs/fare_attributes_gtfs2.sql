{{config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['fare_id', 'feed_start_date'],
    alias = 'fare_attributes'
)}} 


SELECT 
  fi.feed_version,
  SAFE_CAST(fa.data_versao AS DATE) feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(fa.fare_id AS STRING) fare_id,
  SAFE_CAST(JSON_VALUE(fa.content, '$.price') AS FLOAT64) price,
  SAFE_CAST(JSON_VALUE(fa.content, '$.currency_type') AS STRING) currency_type,
  SAFE_CAST(JSON_VALUE(fa.content, '$.payment_method') AS STRING) payment_method,
  SAFE_CAST(JSON_VALUE(fa.content, '$.transfers') AS STRING) transfers,
  SAFE_CAST(JSON_VALUE(fa.content, '$.agency_id') AS STRING) agency_id,
  SAFE_CAST(JSON_VALUE(fa.content, '$.transfer_duration') AS INT64) transfer_duration,
  '{{ var("version") }}' AS versao_modelo
FROM 
  {{source(
    'br_rj_riodejaneiro_gtfs_staging',
    'fare_attributes'
  )}} fa
JOIN
  {{ ref('feed_info_gtfs2') }} fi 
ON 
  fa.data_versao = CAST(fi.feed_start_date AS STRING)
{% if is_incremental() -%}
  WHERE 
    fa.data_versao = '{{ var("data_versao_gtfs") }}'
    AND fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
{%- endif %}
