WITH t AS (
    SELECT SAFE_CAST(feed_publisher_name AS STRING) feed_publisher_name,
        REPLACE(content, "None", "") content,
        --    SAFE_CAST(data_versao AS DATE) data_versao
    FROM { { var('feed_info_gtfs') } }
)
SELECT feed_publisher_name,
    JSON_VALUE(content, "$.feed_publisher_url") feed_publisher_url,
    JSON_VALUE(content, "$.feed_lang") feed_lang,
    JSON_VALUE(content, "$.feed_start_date") feed_start_date,
    JSON_VALUE(content, "$.feed_end_date") feed_end_date,
    JSON_VALUE(content, "$.feed_contact_email") feed_contact_email,
    -- DATE(data_versao) data_versao
FROM t