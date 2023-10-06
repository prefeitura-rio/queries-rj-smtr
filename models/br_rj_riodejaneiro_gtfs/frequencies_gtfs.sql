WITH t AS (
    SELECT SAFE_CAST(trip_id AS STRING) trip_id,
        REPLACE(content, "None", "") content,
        --    SAFE_CAST(data_versao AS DATE) data_versao
    FROM { { var('trip_gtfs') } }
)
SELECT trip_id,
    JSON_VALUE(content, "$.start_time") start_time,
    JSON_VALUE(content, "$.end_time") end_time,
    JSON_VALUE(content, "$.headway_secs") headway_secs,
    JSON_VALUE(content, "$.exact_times") exact_times,
    --   DATE(data_versao) data_versao
FROM t