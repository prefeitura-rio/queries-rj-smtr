{{config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['shape_id', 'shape_pt_sequence', 'feed_start_date'],
    alias = 'shapes'
)}}


SELECT 
  fi.feed_version,
  SAFE_CAST(s.data_versao AS DATE) as feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(s.shape_id AS STRING) shape_id,
  SAFE_CAST(JSON_VALUE(s.content, '$.shape_pt_lat') AS FLOAT64) shape_pt_lat,
  SAFE_CAST(JSON_VALUE(s.content, '$.shape_pt_lon') AS FLOAT64) shape_pt_lon,
  SAFE_CAST(s.shape_pt_sequence AS INT64) shape_pt_sequence,
  SAFE_CAST(JSON_VALUE(s.content, '$.shape_dist_traveled') AS FLOAT64) shape_dist_traveled,
  '{{ var("version") }}' AS versao_modelo
FROM
  {{source('br_rj_riodejaneiro_gtfs_staging', 'shapes')}} s
JOIN 
  {{ ref('feed_info_gtfs2') }} fi 
ON 
  s.data_versao = CAST(fi.feed_start_date AS STRING)
{% if is_incremental() -%}
  WHERE 
    s.data_versao = '{{ var("data_versao_gtfs") }}'
    AND fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
{%- endif %}
