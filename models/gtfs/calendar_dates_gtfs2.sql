{{ config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['service_id', 'date', 'feed_start_date'],
    alias = 'calendar_dates'
) }}


SELECT  
  fi.feed_version,
  SAFE_CAST(cd.data_versao AS DATE) feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(cd.service_id AS STRING) service_id,
  PARSE_DATE('%Y%m%d', SAFE_CAST(cd.DATE AS STRING)) DATE,
  SAFE_CAST(JSON_VALUE(cd.content, '$.exception_type') AS STRING) exception_type,
  '{{ var("version") }}' AS versao_modelo
FROM 
  {{ source(
    'br_rj_riodejaneiro_gtfs_staging',
    'calendar_dates'
  ) }} cd
JOIN 
  {{ ref('feed_info_gtfs2') }} fi 
ON
  cd.data_versao = CAST(fi.feed_start_date AS STRING)
{% if is_incremental() -%}
  WHERE 
    cd.data_versao = '{{ var("data_versao_gtfs") }}'
    AND fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
{%- endif %}
