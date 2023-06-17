{{
  config(
    materialized='view',
  )
}}
SELECT 
    SAFE_CAST(fare_id AS STRING) as fare_id,
    SAFE_CAST(price AS FLOAT64) as price,
    SAFE_CAST(currency_type AS STRING) as currency_type,
    SAFE_CAST(payment_method AS INT64) as payment_method,
    SAFE_CAST(transfers AS INT64) as transfers,
    SAFE_CAST(agency_id AS STRING) as agency_id,
    SAFE_CAST(transfer_duration AS INT64) as transfer_duration
FROM 
  `rj-smtr-dev.gtfs_teste_leone.fare_attributes`
