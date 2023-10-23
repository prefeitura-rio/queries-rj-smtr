{{config(
    partition_by = { 'field' :'data_versao',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['trip_id', 'data_versao'],
    alias = 'stop_times'
)}} 


SELECT SAFE_CAST(data_versao AS DATE) data_versao,
    SAFE_CAST(trip_id AS STRING) trip_id,
    SAFE_CAST(JSON_VALUE(content, '$.arrival_time') AS STRING) arrival_time,
    SAFE_CAST(JSON_VALUE(content, '$.departure_time') AS DATETIME) departure_time,
    SAFE_CAST(JSON_VALUE(content, '$.stop_id') AS STRING) stop_id,
    SAFE_CAST(stop_sequence AS INT64) stop_sequence,
    SAFE_CAST(JSON_VALUE(content, '$.stop_headsign') AS STRING) stop_headsign,
    SAFE_CAST(JSON_VALUE(content, '$.pickup_type') AS STRING) pickup_type,
    SAFE_CAST(JSON_VALUE(content, '$.drop_off_type') AS STRING) drop_off_type,
    SAFE_CAST(JSON_VALUE(content, '$.continuous_pickup') AS STRING) continuous_pickup,
    SAFE_CAST(JSON_VALUE(content, '$.continuous_drop_off') AS STRING) continuous_drop_off,
    SAFE_CAST(JSON_VALUE(content, '$.shape_dist_traveled') AS FLOAT64) shape_dist_traveled,
    SAFE_CAST(JSON_VALUE(content, '$.timepoint') AS STRING) timepoint,
    '{{ var("version") }}' as versao_modelo
FROM {{ source(
            'br_rj_riodejaneiro_gtfs_staging',
            'stop_times'
        ) }}
WHERE data_versao = '{{ var("data_versao_gtfs") }}'
