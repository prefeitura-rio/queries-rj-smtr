{{config( MATERIALIZED = 'incremental',
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['trip_id',
    'data'],
    incremental_strategy = 'insert_overwrite',
    alias = 'trips' )}}

SELECT
  SAFE_CAST(trip_id AS STRING) trip_id,
  SAFE_CAST(data AS DATE) data,
  SAFE_CAST(JSON_VALUE(content, '$.route_id') AS STRING) route_id,
  SAFE_CAST(JSON_VALUE(content, '$.service_id') AS STRING) service_id,
  SAFE_CAST(JSON_VALUE(content, '$.trip_headsign') AS STRING) trip_headsign,
  SAFE_CAST(JSON_VALUE(content, '$.trip_short_name') AS STRING) trip_short_name,
  SAFE_CAST(JSON_VALUE(content, '$.direction_id') AS STRING) direction_id,
  SAFE_CAST(JSON_VALUE(content, '$.shape_id') AS STRING) shape_id,
FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'trips'
        ) }}
WHERE data = '{{ var("data_versao_gtfs") }}'
