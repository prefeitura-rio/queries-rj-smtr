{{
  config(
    materialized='view',
  )
}}

SELECT 
    SAFE_CAST(route_id AS STRING) as route_id,
    SAFE_CAST(agency_id AS STRING) as agency_id,
    SAFE_CAST(route_short_name AS STRING) as route_short_name,
    SAFE_CAST(route_long_name AS STRING) as route_long_name,
    SAFE_CAST(route_desc AS STRING) as route_desc,
    SAFE_CAST(route_type AS INT64) as route_type,
    SAFE_CAST(route_color AS STRING) as route_color,
    SAFE_CAST(route_text_color AS STRING) as route_text_color
FROM 
  `rj-smtr-dev.gtfs_teste_leone.routes`
