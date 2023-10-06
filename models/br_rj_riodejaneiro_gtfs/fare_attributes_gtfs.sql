WITH t AS (
    SELECT SAFE_CAST(fare_id AS STRING) fare_id,
        REPLACE(content, "None", "") content,
        --    SAFE_CAST(data_versao AS DATE) data_versao
    FROM { { var('fare_attributes_staging') } }
)
SELECT service_id,
    JSON_VALUE(content, "$.price") price,
    JSON_VALUE(content, "$.currency_type") currency_type,
    JSON_VALUE(content, "$.payment_method") payment_method,
    JSON_VALUE(content, "$.transfers") transfers,
    JSON_VALUE(content, "$.agency_id") agency_id,
    JSON_VALUE(content, "$.transfer_duration") transfer_duration,
    -- DATE(data_versao) data_versao
FROM t