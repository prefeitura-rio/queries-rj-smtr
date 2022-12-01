
SELECT 
    SAFE_CAST(DATA AS DATE) DATA,
    SAFE_CAST(hora AS INT64) hora,
    SAFE_CAST(SPLIT(trip_id, '_')[OFFSET(0)] AS STRING) trip_id,
    SAFE_CAST(start_time AS TIME) start_time,
    SAFE_CAST(end_time AS TIME) end_time,
    SAFE_CAST(headway_secs AS INT64) headway_secs,
    SAFE_CAST(exact_times AS INT64) exact_times,
    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura
FROM 
    {{ var("frequencies_staging") }} AS t