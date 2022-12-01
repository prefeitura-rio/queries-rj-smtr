SELECT
  SAFE_CAST(data AS DATE) data,
  SAFE_CAST(hora AS INT64) hora,
  SAFE_CAST(route_id AS STRING) route_id,
  SAFE_CAST(service_id AS STRING) service_id,
  SAFE_CAST(SPLIT(trip_id, '_')[
  OFFSET
    (0)] AS STRING) trip_id,
  SAFE_CAST(trip_headsign AS STRING) trip_headsign,
  CONCAT( IFNULL(REGEXP_EXTRACT(trip_short_name, r'[A-Z]+'), ""), IFNULL(REGEXP_EXTRACT(trip_short_name, r'[0-9]+'), "") ) AS trip_short_name,
  CAST(SAFE_CAST(direction_id AS FLOAT64) AS INT64) direction_id,
  SAFE_CAST(block_id AS STRING) block_id,
  SAFE_CAST(shape_id AS STRING) shape_id,
  SAFE_CAST(wheelchair_accessible AS STRING) wheelchair_accessible,
  SAFE_CAST(bikes_allowed AS STRING) bikes_allowed,
  SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura
FROM
  {{ var("trips_staging") }} AS t
