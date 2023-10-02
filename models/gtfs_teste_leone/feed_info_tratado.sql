{{
  config(
    materialized='view',
  )
}}

SELECT 
    SAFE_CAST(feed_publisher_name AS STRING) as feed_publisher_name,
    SAFE_CAST(feed_publisher_url AS STRING) as feed_publisher_url,
    SAFE_CAST(feed_lang AS STRING) as feed_lang,
    PARSE_DATE('%Y%m%d', feed_start_date) as feed_start_date,
    PARSE_DATE('%Y%m%d', feed_end_date) as feed_end_date,
    SAFE_CAST(feed_contact_email AS STRING) as feed_contact_email
FROM 
  `rj-smtr-dev.gtfs_teste_leone.feed_info`
