WITH
  t AS (
  SELECT
    SAFE_CAST(agency_id AS STRING) agency_id,
    REPLACE(content,"None","") content,
--    SAFE_CAST(data_versao AS DATE) data_versao
  FROM
    {{var('agency_staging')}})
SELECT
  agency_id,
  JSON_VALUE(content, "$.agency_name") agency_name,
  JSON_VALUE(content, "$.agency_url") agency_url,
  JSON_VALUE(content, "$.agency_timezone") agency_timezone,
  JSON_VALUE(content, "$.agency_lang") agency_lang,
--   DATE(data_versao) data_versao
FROM
  t