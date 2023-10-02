{{
  config(
    materialized='view',
  )
}}

SELECT 
    SAFE_CAST(trip_id AS STRING) as trip_id,
    SAFE_CAST(stop_sequence AS INT64) as stop_sequence,
    SAFE_CAST(stop_id AS STRING) as stop_id,
    SAFE_CAST(arrival_time AS STRING) as arrival_time,
    SAFE_CAST(departure_time AS STRING) as departure_time,
    SAFE_CAST(stop_headsign AS STRING) as stop_headsign,
    SAFE_CAST(shape_dist_traveled AS FLOAT64) as shape_dist_traveled,
    SAFE_CAST(timepoint AS INT64) as timepoint
FROM 
  `rj-smtr-dev.gtfs_teste_leone.stop_times`
