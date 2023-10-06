WITH t AS (
    SELECT SAFE_CAST(stop_id AS STRING) stop_id,
        REPLACE(content, "None", "") content,
        --    SAFE_CAST(data_versao AS DATE) data_versao
    FROM { { var('stops_staging') } }
)
SELECT service_id,
    JSON_VALUE(content, "$.stop_code") stop_code,
    JSON_VALUE(content, "$.stop_name") stop_name,
    JSON_VALUE(content, "$.stop_desc") stop_desc,
    JSON_VALUE(content, "$.stop_lat") stop_lat,
    JSON_VALUE(content, "$.stop_lon") stop_lon,
    JSON_VALUE(content, "$.zone_id") zone_id,
    JSON_VALUE(content, "$.stop_url") stop_url,
    JSON_VALUE(content, "$.location_type") location_type,
    JSON_VALUE(content, "$.parent_station") parent_station,
    JSON_VALUE(content, "$.stop_timezone") stop_timezone,
    JSON_VALUE(content, "$.wheelchair_boarding") wheelchair_boarding,
    JSON_VALUE(content, "$.platform_code") platform_code,
    -- DATE(data_versao) data_versao
FROM t