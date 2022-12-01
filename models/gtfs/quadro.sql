SELECT
  SAFE_CAST(DATA AS DATE) DATA,
  SAFE_CAST(hora AS INT64) hora,
  CONCAT( IFNULL(REGEXP_EXTRACT(trip_short_name, r'[A-Z]+'), ""), IFNULL(REGEXP_EXTRACT(trip_short_name, r'[0-9]+'), "") ) AS trip_short_name,
  SAFE_CAST(route_long_name AS STRING) route_long_name,
  SAFE_CAST(agency_name AS STRING) agency_name,
  SAFE_CAST(start_time AS TIME) start_time,
  SAFE_CAST(end_time AS TIME) end_time,
  SAFE_CAST(direction_id AS INT64) direction_id,
  SAFE_CAST(trip_id AS STRING) trip_id,
  ROUND(SAFE_CAST(shape_distance AS FLOAT64),3) shape_distance,
  SAFE_CAST(service_id AS STRING) service_id,
  ROUND(SAFE_CAST(trip_daily_distance AS FLOAT64),3) trip_daily_distance,
  SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura
FROM
  {{ var("quadro_staging") }} AS t