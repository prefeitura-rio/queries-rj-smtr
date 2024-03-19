{{config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['stop_id', 'feed_start_date'],
    alias = 'stops'
)}} 


SELECT 
    fi.feed_version,
    SAFE_CAST(s.data_versao AS DATE) as feed_start_date,
    fi.feed_end_date,
    SAFE_CAST(s.stop_id AS STRING) stop_id,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_code') AS STRING) stop_code,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_name') AS STRING) stop_name,
    SAFE_CAST(JSON_VALUE(s.content, '$.tts_stop_name') AS STRING) tts_stop_name,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_desc') AS STRING) stop_desc,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_lat') AS FLOAT64) stop_lat,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_lon') AS FLOAT64) stop_lon,
    SAFE_CAST(JSON_VALUE(s.content, '$.zone_id') AS STRING) zone_id,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_url') AS STRING) stop_url,
    SAFE_CAST(SAFE_CAST(SAFE_CAST(JSON_VALUE(s.content, '$.location_type') AS FLOAT64) AS INT64) AS STRING) location_type,
    SAFE_CAST(JSON_VALUE(s.content, '$.parent_station') AS STRING) parent_station,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_timezone') AS STRING) stop_timezone,
    SAFE_CAST(JSON_VALUE(s.content, '$.wheelchair_boarding') AS STRING) wheelchair_boarding,
    SAFE_CAST(JSON_VALUE(s.content, '$.level_id') AS STRING) level_id,
    SAFE_CAST(JSON_VALUE(s.content, '$.platform_code') AS STRING) platform_code,
    '{{ var("version") }}' AS versao_modelo
FROM 
    {{ source(
        'br_rj_riodejaneiro_gtfs_staging',
        'stops'
    ) }} s
JOIN
    {{ ref('feed_info_gtfs2') }} fi 
ON 
    s.data_versao = CAST(fi.feed_start_date AS STRING)
{% if is_incremental() -%}
    WHERE 
        s.data_versao = '{{ var("data_versao_gtfs") }}'
        AND fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
{%- endif %}
