{{config(
    partition_by = { 'field' :'data_versao',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['stop_id', 'data_versao'],
    alias = 'stops'
)}} 


SELECT SAFE_CAST(data_versao AS DATE) data_versao,
    SAFE_CAST(stop_id AS STRING) stop_id,
    SAFE_CAST(JSON_VALUE(content, '$.stop_code') AS STRING) stop_code,
    SAFE_CAST(JSON_VALUE(content, '$.stop_name') AS STRING) stop_name,
    SAFE_CAST(JSON_VALUE(content, '$.tts_stop_name') AS STRING) tts_stop_name,
    SAFE_CAST(JSON_VALUE(content, '$.stop_desc') AS STRING) stop_desc,
    SAFE_CAST(JSON_VALUE(content, '$.stop_lat') AS FLOAT64) stop_lat,
    SAFE_CAST(JSON_VALUE(content, '$.stop_lon') AS FLOAT64) stop_lon,
    SAFE_CAST(JSON_VALUE(content, '$.zone_id') AS STRING) zone_id,
    SAFE_CAST(JSON_VALUE(content, '$.stop_url') AS STRING) stop_url,
    SAFE_CAST(SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.location_type') AS FLOAT64) AS INT64) AS STRING) location_type,
    SAFE_CAST(JSON_VALUE(content, '$.parent_station') AS STRING) parent_station,
    SAFE_CAST(JSON_VALUE(content, '$.stop_timezone') AS STRING) stop_timezone,
    SAFE_CAST(JSON_VALUE(content, '$.wheelchair_boarding') AS STRING) wheelchair_boarding,
    SAFE_CAST(JSON_VALUE(content, '$.level_id') AS STRING) level_id,
    SAFE_CAST(JSON_VALUE(content, '$.platform_code') AS STRING) platform_code,
    '{{ var("version") }}' as versao_modelo
FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'stops'
        ) }}
WHERE data_versao = '{{ var("data_versao_gtfs") }}'
