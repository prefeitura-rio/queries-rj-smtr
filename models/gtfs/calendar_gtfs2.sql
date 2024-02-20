{{config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['service_id', 'feed_start_date'],
    alias = 'calendar'
)}} 


SELECT 
    SAFE_CAST(data_versao AS DATE) feed_start_date,
    SAFE_CAST(service_id AS STRING) service_id,
    SAFE_CAST(JSON_VALUE(content, '$.monday') AS STRING) monday,
    SAFE_CAST(JSON_VALUE(content, '$.tuesday') AS STRING) tuesday,
    SAFE_CAST(JSON_VALUE(content, '$.wednesday') AS STRING) wednesday,
    SAFE_CAST(JSON_VALUE(content, '$.thursday') AS STRING) thursday,
    SAFE_CAST(JSON_VALUE(content, '$.friday') AS STRING) friday,
    SAFE_CAST(JSON_VALUE(content, '$.saturday') AS STRING) saturday,
    SAFE_CAST(JSON_VALUE(content, '$.sunday') AS STRING) sunday,
    PARSE_DATE('%Y%m%d', SAFE_CAST(JSON_VALUE(content, '$.start_date') AS STRING)) start_date,
    PARSE_DATE('%Y%m%d', SAFE_CAST(JSON_VALUE(content, '$.end_date') AS STRING)) end_date,
    '{{ var("version") }}' as versao_modelo
 FROM {{ source(
            'gtfs_staging',
            'calendar'
        ) }}
  {% if is_incremental() -%}
    WHERE data_versao = '{{ var("data_versao_gtfs") }}'
  {%- endif %}
