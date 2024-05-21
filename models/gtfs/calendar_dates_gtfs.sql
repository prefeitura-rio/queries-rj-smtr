{{ config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['service_id', 'date', 'feed_start_date'],
    alias = 'calendar_dates'
) }}

{% if execute and is_incremental() %}
  {% set results = run_query("SELECT MAX(feed_start_date) FROM " ~ ref('feed_info_gtfs') ~ " WHERE feed_start_date < " ~ '{{ var("data_versao_gtfs") }}') %}
  {% set last_feed_version = results.columns[0].values()[0] %}
{% endif %}


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
  {{ ref('feed_info_gtfs') }} fi 
ON
  cd.data_versao = CAST(fi.feed_start_date AS STRING)
{% if is_incremental() -%}
  WHERE 
    cd.data_versao IN ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
    AND fi.feed_start_date IN ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
