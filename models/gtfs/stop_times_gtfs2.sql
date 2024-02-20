{{config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['trip_id', 'feed_start_date'],
    alias = 'stop_times'
)}} 


SELECT SAFE_CAST(data_versao AS DATE) as feed_start_date,
    SAFE_CAST(trip_id AS STRING) trip_id,
    SAFE_CAST(JSON_VALUE(content, '$.arrival_time') AS STRING) arrival_time,
    SAFE_CAST(JSON_VALUE(content, '$.departure_time') AS DATETIME) departure_time,
    SAFE_CAST(JSON_VALUE(content, '$.stop_id') AS STRING) stop_id,
    SAFE_CAST(stop_sequence AS INT64) stop_sequence,
    SAFE_CAST(JSON_VALUE(content, '$.stop_headsign') AS STRING) stop_headsign,
    SAFE_CAST(JSON_VALUE(content, '$.pickup_type') AS STRING) pickup_type,
    SAFE_CAST(JSON_VALUE(content, '$.drop_off_type') AS STRING) drop_off_type,
    SAFE_CAST(JSON_VALUE(content, '$.continuous_pickup') AS STRING) continuous_pickup,
    SAFE_CAST(JSON_VALUE(content, '$.continuous_drop_off') AS STRING) continuous_drop_off,
    SAFE_CAST(JSON_VALUE(content, '$.shape_dist_traveled') AS FLOAT64) shape_dist_traveled,
    SAFE_CAST(JSON_VALUE(content, '$.timepoint') AS STRING) timepoint,
    '{{ var("version") }}' as versao_modelo
FROM {{ source(
            'gtfs_staging',
            'stop_times'
        ) }}
  {% if is_incremental() -%}
    WHERE data_versao = '{{ var("data_versao_gtfs") }}'
  {%- endif %}
