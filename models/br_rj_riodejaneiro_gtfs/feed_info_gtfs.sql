{{config(
    materialized = "incremental",
    partition_by = { "field" :"data",
    "data_type" :"date",
    "granularity": "day" },
    unique_key = ["feed_publisher_name", "data"],
    incremental_strategy = "insert_overwrite",
    alias = 'feed_info'
)}} 

WITH t AS (
    SELECT SAFE_CAST(feed_publisher_name AS STRING) feed_publisher_name,
        REPLACE(content, "None", "") content,
        SAFE_CAST(data AS DATE) data
    FROM {{ source('br_rj_riodejaneiro_gtfs_staging', 'feed_info') }}
)

SELECT feed_publisher_name,
    JSON_VALUE(content, "$.feed_publisher_url") feed_publisher_url,
    JSON_VALUE(content, "$.feed_lang") feed_lang,
    JSON_VALUE(content, "$.feed_start_date") feed_start_date,
    JSON_VALUE(content, "$.feed_end_date") feed_end_date,
    JSON_VALUE(content, "$.feed_contact_email") feed_contact_email,
    DATE(data) data
FROM t
