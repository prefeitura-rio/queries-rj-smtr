{{ config(
  partition_by = { 'field' :'feed_start_date',
  'data_type' :'date',
  'granularity': 'day' },
  unique_key = ['agency_id', 'feed_start_date'],
  alias = 'agency',
) }} 


SELECT 
  fi.feed_version,
  SAFE_CAST(a.data_versao AS DATE) feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(a.agency_id AS STRING) agency_id,
  SAFE_CAST(JSON_VALUE(a.content, '$.agency_name') AS STRING) agency_name,
  SAFE_CAST(JSON_VALUE(a.content, '$.agency_url') AS STRING) agency_url,
  SAFE_CAST(JSON_VALUE(a.content, '$.agency_timezone') AS STRING) agency_timezone,
  SAFE_CAST(JSON_VALUE(a.content, '$.agency_lang') AS STRING) agency_lang,
  '{{ var("version") }}' AS versao_modelo
FROM 
  {{ source('br_rj_riodejaneiro_gtfs_staging', 'agency') }} a
JOIN 
  {{ ref('feed_info_gtfs2') }} fi 
ON 
  a.data_versao = CAST(fi.feed_start_date AS STRING)
{% if is_incremental() -%}
  WHERE 
    a.data_versao = '{{ var("data_versao_gtfs") }}'
    AND fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
{%- endif %}