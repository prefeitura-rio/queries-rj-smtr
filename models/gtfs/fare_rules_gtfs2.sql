{{config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['fare_id', 'feed_start_date'],
    alias = 'fare_rules'
)}} 


SELECT
  fi.feed_version,
  SAFE_CAST(fr.data_versao AS DATE) feed_start_date,
  fi.feed_end_date, 
  SAFE_CAST(JSON_VALUE(fr.content, '$.fare_id') AS STRING) fare_id,
  SAFE_CAST(JSON_VALUE(fr.content, '$.route_id') AS STRING) route_id,
  SAFE_CAST(JSON_VALUE(fr.content, '$.origin_id') AS STRING) origin_id,
  SAFE_CAST(JSON_VALUE(fr.content, '$.destination_id') AS STRING) destination_id,
  SAFE_CAST(JSON_VALUE(fr.content, '$.contains_id') AS STRING) contains_id,
  '{{ var("version") }}' AS versao_modelo
FROM
  {{ source(
    'br_rj_riodejaneiro_gtfs_staging',
    'fare_rules'
  ) }} fr
JOIN
  {{ ref('feed_info_gtfs2') }} fi 
ON 
  fr.data_versao = CAST(fi.feed_start_date AS STRING)
{% if is_incremental() -%}
  WHERE 
    fr.data_versao = '{{ var("data_versao_gtfs") }}'
    AND fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
{%- endif %}