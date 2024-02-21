{{ config(
  partition_by = { 'field' :'feed_start_date',
  'data_type' :'date',
  'granularity': 'day' },
  unique_key = ['agency_id', 'feed_start_date'],
  alias = 'agency',
) }} 


SELECT 
  SAFE_CAST(data_versao AS DATE) feed_start_date,
  SAFE_CAST(agency_id AS STRING) agency_id,
  SAFE_CAST(JSON_VALUE(content, '$.agency_name') AS STRING) agency_name,
  SAFE_CAST(JSON_VALUE(content, '$.agency_url') AS STRING) agency_url,
  SAFE_CAST(JSON_VALUE(content, '$.agency_timezone') AS STRING) agency_timezone,
  SAFE_CAST(JSON_VALUE(content, '$.agency_lang') AS STRING) agency_lang,
  '{{ var("version") }}' as versao_modelo
  FROM {{ source('gtfs_staging', 'agency') }}
  {% if is_incremental() -%}
    WHERE data_versao = '{{ var("data_versao_gtfs") }}'
  {%- endif %}
