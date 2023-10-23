{{config(
    partition_by = { 'field' :'data_versao',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['shape_id', 'shape_pt_sequence', 'data_versao'],
    alias = 'shapes'
)}}


SELECT SAFE_CAST(data_versao AS DATE) data_versao,
  SAFE_CAST(shape_id AS STRING) shape_id,
  SAFE_CAST(JSON_VALUE(content, '$.shape_pt_lat') AS FLOAT64) shape_pt_lat,
  SAFE_CAST(JSON_VALUE(content, '$.shape_pt_lon') AS FLOAT64) shape_pt_lon,
  SAFE_CAST(shape_pt_sequence AS INT64) shape_pt_sequence,
  SAFE_CAST(JSON_VALUE(content, '$.shape_dist_traveled') AS FLOAT64) shape_dist_traveled,
  '{{ var("version") }}' as versao_modelo
FROM
  {{source('br_rj_riodejaneiro_gtfs_staging', 'shapes')}}
WHERE data_versao = '{{ var("data_versao_gtfs") }}'
