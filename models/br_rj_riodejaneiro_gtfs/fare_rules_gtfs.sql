{{config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['fare_id', 'feed_start_date'],
    alias = 'fare_rules'
)}} 


SELECT
    SAFE_CAST(data_versao AS DATE) feed_start_date,
    SAFE_CAST(JSON_VALUE(content, '$.fare_id') AS STRING) fare_id,
    SAFE_CAST(JSON_VALUE(content, '$.route_id') AS STRING) route_id,
    SAFE_CAST(JSON_VALUE(content, '$.origin_id') AS STRING) origin_id,
    SAFE_CAST(JSON_VALUE(content, '$.destination_id') AS STRING) destination_id,
    SAFE_CAST(JSON_VALUE(content, '$.contains_id') AS STRING) contains_id,
    '{{ var("version") }}' as versao_modelo
 FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'fare_rules'
        ) }}
  {% if is_incremental() -%}
    WHERE data_versao = '{{ var("data_versao_gtfs") }}'
  {%- endif %}
