SELECT 
    SAFE_CAST(data AS STRING) data,
    SAFE_CAST(hora AS STRING) hora,
    SAFE_CAST(agency_id AS STRING) agency_id,
    SAFE_CAST(agency_name AS STRING) agency_name,
    SAFE_CAST(agency_url AS STRING) agency_url,
    SAFE_CAST(agency_timezone AS STRING) agency_timezone,
    SAFE_CAST(agency_lang AS STRING) agency_lang,
    SAFE_CAST(agency_phone AS STRING) agency_phone,
    SAFE_CAST(agency_branding_url AS STRING) agency_branding_url,
    SAFE_CAST(agency_fare_url AS STRING) agency_fare_url,
    SAFE_CAST(agency_email AS STRING) agency_email,
    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura
FROM 
    {{ var("agency_staging") }} AS t