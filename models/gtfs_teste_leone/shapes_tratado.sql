{{
  config(
    materialized='view',
  )
}}

SELECT 
    SAFE_CAST(shape_id AS STRING) as shape_id,
    SAFE_CAST(shape_pt_sequence AS INT64) as shape_pt_sequence,
    SAFE_CAST(shape_pt_lat AS FLOAT64) as shape_pt_lat,
    SAFE_CAST(shape_pt_lon AS FLOAT64) as shape_pt_lon,
    SAFE_CAST(shape_dist_traveled AS FLOAT64) as shape_dist_traveled
FROM 
  `rj-smtr-dev.gtfs_teste_leone.shapes`

