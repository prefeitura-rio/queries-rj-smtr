{{
  config(
    materialized='view',
  )
}}

SELECT 
    SAFE_CAST(trip_id AS STRING) as trip_id,
    SAFE_CAST(route_id AS STRING) as route_id,
    SAFE_CAST(service_id AS STRING) as service_id,
    SAFE_CAST(trip_headsign AS STRING) as trip_headsign,
    SAFE_CAST(trip_short_name AS STRING) as trip_short_name,
    SAFE_CAST(direction_id AS INT64) as direction_id,
    SAFE_CAST(shape_id AS STRING) as shape_id
FROM 
  `rj-smtr-dev.gtfs_teste_leone.trips`
