WITH t AS (
    SELECT SAFE_CAST(shape_id AS STRING) shape_id,
        REPLACE(content, "None", "") content,
        --    SAFE_CAST(data_versao AS DATE) data_versao
    FROM { { var('shapes_gtfs') } }
)
SELECT shape_id,
    JSON_VALUE(content, "$.shape_pt_sequence") shape_pt_sequence,
    JSON_VALUE(content, "$.shape_pt_lat") shape_pt_lat,
    JSON_VALUE(content, "$.shape_pt_lon") shape_pt_lon,
    JSON_VALUE(content, "$.shape_dist_traveled") shape_dist_traveled,
    -- DATE(data_versao) data_versao
FROM t