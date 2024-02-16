{{config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['feed_publisher_name', 'feed_start_date'],
    alias = 'feed_info'
)}} 


SELECT 
  SAFE_CAST(timestamp_captura AS STRING) AS feed_version, 
  SAFE_CAST(data_versao AS DATE) as feed_start_date,
  date_sub(lead(date(data_versao)) over (order by data_versao), interval 1 day) as feed_end_date,
  SAFE_CAST(feed_publisher_name AS STRING) feed_publisher_name,
  SAFE_CAST(JSON_VALUE(content, '$.feed_publisher_url') AS STRING) feed_publisher_url,
  SAFE_CAST(JSON_VALUE(content, '$.feed_lang') AS STRING) feed_lang,
  SAFE_CAST(JSON_VALUE(content, '$.default_lang') AS STRING) default_lang,
  SAFE_CAST(JSON_VALUE(content, '$.feed_contact_email') AS STRING) feed_contact_email,
  SAFE_CAST(JSON_VALUE(content, '$.feed_contact_url') AS STRING) feed_contact_url,
    '{{ var("version") }}' as versao_modelo
 FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'feed_info'
        ) }}
  {% if is_incremental() -%}
    WHERE data_versao = '{{ var("data_versao_gtfs") }}'
  {%- endif %}
