{{
  config(
    materialized='view',
  )
}}

SELECT 
    SAFE_CAST(fare_id AS STRING) as fare_id,
    SAFE_CAST(route_id AS STRING) as route_id
FROM 
  `rj-smtr-dev.gtfs_teste_leone.fare_rules`
