{{
  config(
    materialized='view',
  )
}}

SELECT 
    SAFE_CAST(stop_id AS STRING) as stop_id,
    SAFE_CAST(stop_code AS STRING) as stop_code,
    SAFE_CAST(stop_name AS STRING) as stop_name,
    SAFE_CAST(stop_desc AS STRING) as stop_desc,
    SAFE_CAST(stop_lat AS FLOAT64) as stop_lat,
    SAFE_CAST(stop_lon AS FLOAT64) as stop_lon,
    SAFE_CAST(zone_id AS STRING) as zone_id,
    SAFE_CAST(stop_url AS STRING) as stop_url,
    SAFE_CAST(location_type AS INT64) as location_type,
    SAFE_CAST(parent_station AS STRING) as parent_station,
    SAFE_CAST(stop_timezone AS STRING) as stop_timezone,
    SAFE_CAST(wheelchair_boarding AS INT64) as wheelchair_boarding,
    SAFE_CAST(platform_code AS STRING) as platform_code
FROM 
  `rj-smtr-dev.gtfs_teste_leone.stops`
