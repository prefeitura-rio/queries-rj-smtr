{{config(
    materialized = 'incremental',
    partition_by = { 'field' :'data',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['shape_id', 'shape_pt_sequence', 'data'],
    incremental_strategy = 'insert_overwrite',
    alias = 'shapes'
)}}


SELECT
  SAFE_CAST(shape_id AS STRING) shape_id,
  SAFE_CAST(shape_pt_sequence AS FLOAT64) shape_pt_sequence,
  SAFE_CAST(DATA AS DATE) data,
  SAFE_CAST(JSON_VALUE(content, '$.shape_pt_lat') AS FLOAT64) shape_pt_lat,
  SAFE_CAST(JSON_VALUE(content, '$.shape_pt_lon') AS FLOAT64) shape_pt_lon,
  SAFE_CAST(JSON_VALUE(content, '$.shape_dist_traveled') AS FLOAT64) shape_dist_traveled,
  
 FROM
  {{source('br_rj_riodejaneiro_gtfs_staging', 'shapes')}}
  
WHERE data = '{{ var("data_versao_gtfs") }}'
