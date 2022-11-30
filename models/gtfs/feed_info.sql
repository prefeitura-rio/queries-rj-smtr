
SELECT
  SAFE_CAST(DATA AS DATE) DATA,
  SAFE_CAST(hora AS INT64) hora,
  SAFE_CAST(feed_publisher_name AS STRING) feed_publisher_name,
  SAFE_CAST(feed_id AS STRING) feed_id,
  SAFE_CAST(feed_publisher_url AS STRING) feed_publisher_url,
  SAFE_CAST(feed_lang AS STRING) feed_lang,
  SAFE_CAST(PARSE_DATE('%Y%m%d', feed_start_date) AS DATE) feed_start_date,
  SAFE_CAST(PARSE_DATE('%Y%m%d', feed_end_date) AS DATE) feed_end_date,
  SAFE_CAST(feed_version AS STRING) feed_version,
  SAFE_CAST(default_lang AS STRING) default_lang,
  SAFE_CAST(feed_contact_email AS STRING) feed_contact_email,
  SAFE_CAST(feed_contact_url AS STRING) feed_contact_url,
  SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura
FROM
  {{ var("feed_info_staging") }} AS t