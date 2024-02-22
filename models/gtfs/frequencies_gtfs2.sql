{{config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['trip_id', 'start_time', 'feed_start_date'],
    alias = 'frequencies'
)}} 


SELECT SAFE_CAST(data_versao AS DATE) as feed_start_date,
       SAFE_CAST(trip_id AS STRING) trip_id,
       SAFE_CAST(start_time AS STRING) start_time,
       SAFE_CAST(JSON_VALUE(content, '$.end_time') AS STRING) end_time,
       SAFE_CAST(JSON_VALUE(content, '$.headway_secs') AS INT64) headway_secs,
       SAFE_CAST(JSON_VALUE(content, '$.exact_times') AS STRING) exact_times,
       '{{ var("version") }}' as versao_modelo
 FROM {{source('br_rj_riodejaneiro_gtfs_staging', 'frequencies')}}
  {% if is_incremental() -%}
    WHERE data_versao = '{{ var("data_versao_gtfs") }}'
  {%- endif %}
