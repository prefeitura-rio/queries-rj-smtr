WITH t AS (
    SELECT SAFE_CAST(route_id AS STRING) route_id,
        REPLACE(content, "None", "") content,
        --    SAFE_CAST(data_versao AS DATE) data_versao
    FROM { { var('routes_staging') } }
)
SELECT service_id,
    JSON_VALUE(content, "$.agency_id") agency_id,
    JSON_VALUE(content, "$.route_short_name") route_short_name,
    JSON_VALUE(content, "$.route_long_name") route_long_name,
    JSON_VALUE(content, "$.route_desc") route_desc,
    JSON_VALUE(content, "$.route_type") route_type,
    JSON_VALUE(content, "$.route_color") route_color,
    JSON_VALUE(content, "$.route_text_color") route_text_color,
    -- DATE(data_versao) data_versao
FROM t