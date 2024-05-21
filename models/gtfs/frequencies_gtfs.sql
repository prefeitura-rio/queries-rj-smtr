{{config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['trip_id', 'start_time', 'feed_start_date'],
    alias = 'frequencies'
)}} 

{% if execute and is_incremental() %}
  {% set results = run_query("SELECT MAX(feed_start_date) FROM " ~ ref('feed_info_gtfs') ~ " WHERE feed_start_date < " ~ '{{ var("data_versao_gtfs") }}') %}
  {% set last_feed_version = results.columns[0].values()[0] %}
{% endif %}

SELECT 
  fi.feed_version,  
  SAFE_CAST(f.data_versao AS DATE) as feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(f.trip_id AS STRING) trip_id,
  SAFE_CAST(f.start_time AS STRING) start_time,
  SAFE_CAST(JSON_VALUE(f.content, '$.end_time') AS STRING) end_time,
  SAFE_CAST(JSON_VALUE(f.content, '$.headway_secs') AS INT64) headway_secs,
  SAFE_CAST(JSON_VALUE(f.content, '$.exact_times') AS STRING) exact_times,
  '{{ var("version") }}' AS versao_modelo
FROM 
  {{source('br_rj_riodejaneiro_gtfs_staging', 'frequencies')}} f
JOIN 
  {{ ref('feed_info_gtfs') }} fi 
ON 
  f.data_versao = CAST(fi.feed_start_date AS STRING)
{% if is_incremental() -%}
  WHERE 
    f.data_versao IN ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
    AND fi.feed_start_date IN ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
