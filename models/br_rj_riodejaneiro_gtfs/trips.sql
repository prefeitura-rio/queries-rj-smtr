WITH t AS (
    SELECT SAFE_CAST(trip_id AS STRING) trip_id,
        REPLACE(content, "None", "") content,
        --    SAFE_CAST(data_versao AS DATE) data_versao
    FROM { { var('trips_staging') } }
)
SELECT service_id,
    JSON_VALUE(content, "$.route_id") route_id,
    JSON_VALUE(content, "$.service_id") service_id,
    JSON_VALUE(content, "$.trip_headsign") trip_headsign,
    JSON_VALUE(content, "$.trip_short_name") trip_short_name,
    JSON_VALUE(content, "$.direction_id") direction_id,
    JSON_VALUE(content, "$.shape_id") shape_id,
    -- DATE(data_versao) data_versao
FROM t