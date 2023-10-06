WITH t AS (
    SELECT SAFE_CAST(service_id AS STRING) service_id,
        REPLACE(content, "None", "") content,
        --    SAFE_CAST(data_versao AS DATE) data_versao
    FROM { { var('calendar_gtfs') } }
)
SELECT service_id,
    JSON_VALUE(content, "$.monday") monday,
    JSON_VALUE(content, "$.tuesday") tuesday,
    JSON_VALUE(content, "$.wednesday") wednesday,
    JSON_VALUE(content, "$.thursday") thursday,
    JSON_VALUE(content, "$.friday") friday,
    JSON_VALUE(content, "$.saturday") saturday,
    JSON_VALUE(content, "$.sunday") sunday,
    JSON_VALUE(content, "$.start_date") start_date,
    JSON_VALUE(content, "$.end_date") end_date,
    -- DATE(data_versao) data_versao
FROM t