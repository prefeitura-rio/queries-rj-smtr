{{
  config(
    materialized='view',
  )
}}
SELECT 
  SAFE_CAST(service_id AS STRING) as service_id,
  PARSE_DATE('%Y%m%d', date) as date,
  IF(SAFE_CAST(exception_type AS STRING) = '1', TRUE, FALSE) AS exception_type
FROM 
  `rj-smtr-dev.gtfs_teste_leone.calendar_dates`
