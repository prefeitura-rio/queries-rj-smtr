SELECT
  SAFE_CAST(DATA AS DATE) DATA,
  SAFE_CAST(hora AS INT64) hora,
  SAFE_CAST(service_id AS STRING) service_id,
  SAFE_CAST(PARSE_DATE('%Y%m%d', date) AS DATE) date,
  SAFE_CAST(exception_type AS STRING) exception_type,
  SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura
FROM
  {{ var("calendar_dates_staging") }} AS t