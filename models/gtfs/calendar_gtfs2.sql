{{config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['service_id', 'feed_start_date'],
    alias = 'calendar'
)}} 


SELECT 
  fi.feed_version,
  SAFE_CAST(c.data_versao AS DATE) feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(c.service_id AS STRING) service_id,
  SAFE_CAST(JSON_VALUE(c.content, '$.monday') AS STRING) monday,
  SAFE_CAST(JSON_VALUE(c.content, '$.tuesday') AS STRING) tuesday,
  SAFE_CAST(JSON_VALUE(c.content, '$.wednesday') AS STRING) wednesday,
  SAFE_CAST(JSON_VALUE(c.content, '$.thursday') AS STRING) thursday,
  SAFE_CAST(JSON_VALUE(c.content, '$.friday') AS STRING) friday,
  SAFE_CAST(JSON_VALUE(c.content, '$.saturday') AS STRING) saturday,
  SAFE_CAST(JSON_VALUE(c.content, '$.sunday') AS STRING) sunday,
  PARSE_DATE('%Y%m%d', SAFE_CAST(JSON_VALUE(c.content, '$.start_date') AS STRING)) start_date,
  PARSE_DATE('%Y%m%d', SAFE_CAST(JSON_VALUE(c.content, '$.end_date') AS STRING)) end_date,
  '{{ var("version") }}' AS versao_modelo
 FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'calendar'
        ) }} c
JOIN
  {{ ref('feed_info_gtfs2') }} fi 
ON 
  c.data_versao = CAST(fi.feed_start_date AS STRING)
{% if is_incremental() -%}
  WHERE 
    c.data_versao = '{{ var("data_versao_gtfs") }}'
    AND fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
{%- endif %}
