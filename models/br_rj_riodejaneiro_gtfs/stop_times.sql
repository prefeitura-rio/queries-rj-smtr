WITH t AS (
    SELECT SAFE_CAST(trip_id AS STRING) trip_id,
        REPLACE(content, "None", "") content,
        --    SAFE_CAST(data_versao AS DATE) data_versao
    FROM { { var('stop_times_staging') } }
)
SELECT service_id,
    JSON_VALUE(content, "$.stop_sequence") stop_sequence,
    JSON_VALUE(content, "$.stop_id") stop_id,
    JSON_VALUE(content, "$.arrival_time") arrival_time,
    JSON_VALUE(content, "$.departure_time") departure_time,
    JSON_VALUE(content, "$.stop_headsign") stop_headsign,
    JSON_VALUE(content, "$.shape_dist_traveled") shape_dist_traveled,
    JSON_VALUE(content, "$.timepoint") timepoint,
    -- DATE(data_versao) data_versao
FROM t