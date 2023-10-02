{{
  config(
    materialized='view',
  )
}}
SELECT
  service_id,
  CAST(SAFE_CAST(monday AS INT64) AS BOOL) AS monday,
  CAST(SAFE_CAST(tuesday AS INT64) AS BOOL) AS tuesday,
  CAST(SAFE_CAST(wednesday AS INT64) AS BOOL) AS wednesday,
  CAST(SAFE_CAST(thursday AS INT64) AS BOOL) AS thursday,
  CAST(SAFE_CAST(friday AS INT64) AS BOOL) AS friday,
  CAST(SAFE_CAST(saturday AS INT64) AS BOOL) AS saturday,
  CAST(SAFE_CAST(sunday AS INT64) AS BOOL) AS sunday,
  PARSE_DATE('%Y%m%d', start_date) AS start_date,
  PARSE_DATE('%Y%m%d', end_date) AS end_date,
FROM
  `rj-smtr-dev.gtfs_teste_leone.calendar`
  