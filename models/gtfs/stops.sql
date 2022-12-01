
SELECT 
    SAFE_CAST(DATA AS DATE) DATA,
    SAFE_CAST(hora AS INT64) hora,
    SAFE_CAST(stop_id AS STRING) stop_id,
    SAFE_CAST(stop_code AS STRING) stop_code,
    SAFE_CAST(stop_name AS STRING) stop_name,
    SAFE_CAST(stop_desc AS STRING) stop_desc,
    SAFE_CAST(stop_lat AS FLOAT64) stop_lat,
    SAFE_CAST(stop_lon AS FLOAT64) stop_lon,
    SAFE_CAST(zone_id AS STRING) zone_id,
    SAFE_CAST(stop_url AS STRING) stop_url,
    SAFE_CAST(location_type AS INT64) location_type,
    SAFE_CAST(parent_station AS STRING) parent_station,
    SAFE_CAST(stop_timezone AS STRING) stop_timezone,
    SAFE_CAST(wheelchair_boarding AS INT64) wheelchair_boarding,
    SAFE_CAST(platform_code AS STRING) platform_code,
    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura
FROM {{ var("stops_staging") }} AS t