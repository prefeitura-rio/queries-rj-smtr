WITH t AS (
    SELECT SAFE_CAST(fare_id AS STRING) fare_id,
        REPLACE(content, "None", "") content,
        --    SAFE_CAST(data_versao AS DATE) data_versao
    FROM { { var('fare_rules_gtfs') } }
)
SELECT fare_id,
    JSON_VALUE(content, "$.route_id") route_id,
    JSON_VALUE(content, "$.agency_id") agency_id,
    -- DATE(data_versao) data_versao
FROM t