SELECT 
    SAFE_CAST(data AS DATE) data,
    SAFE_CAST(hora AS INT64) hora,
    SAFE_CAST(service_id AS STRING) service_id,
    SAFE_CAST(monday AS INT64) monday,
    SAFE_CAST(tuesday AS INT64) tuesday,
    SAFE_CAST(wednesday AS INT64) wednesday,
    SAFE_CAST(thursday AS INT64) thursday,
    SAFE_CAST(friday AS INT64) friday,
    SAFE_CAST(saturday AS INT64) saturday,
    SAFE_CAST(sunday AS INT64) sunday,
    SAFE_CAST(PARSE_DATE('%Y%m%d', start_date) AS DATE) start_date,
    SAFE_CAST(PARSE_DATE('%Y%m%d', end_date) AS DATE) end_date,
    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura
FROM {{ var("calendar_staging") }} AS t