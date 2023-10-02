{{
  config(
    materialized='view',
  )
}}

SELECT 
    SAFE_CAST(trip_id AS STRING) as trip_id,
    SAFE_CAST(start_time AS STRING) as start_time,
    SAFE_CAST(end_time AS STRING) as end_time,
    SAFE_CAST(headway_secs AS INT64) as headway_secs,
    SAFE_CAST(exact_times AS INT64) as exact_times
FROM 
  `rj-smtr-dev.gtfs_teste_leone.frequencies`
