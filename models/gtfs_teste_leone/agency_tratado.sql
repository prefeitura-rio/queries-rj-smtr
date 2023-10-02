{{
  config(
    materialized='view',
  )
}}

SELECT 
    SAFE_CAST(agency_id AS STRING) as agency_id,
    SAFE_CAST(agency_name AS STRING) as agency_name,
    SAFE_CAST(agency_url AS STRING) as agency_url,
    SAFE_CAST(agency_timezone AS STRING) as agency_timezone,
    SAFE_CAST(agency_lang AS STRING) as agency_lang
FROM 
  `rj-smtr-dev.gtfs_teste_leone.agency`
