
SELECT
  agency_id,
  JSON_VALUE(content, "$.agency_name") agency_name,
  JSON_VALUE(content, "$.agency_url") agency_url,
  JSON_VALUE(content, "$.agency_timezone") agency_timezone,
  JSON_VALUE(content, "$.agency_lang") agency_lang,
  JSON_VALUE(content, "$.agency_phone") agency_phone,
  DATE(data_versao) data_versao
FROM {{ ref('agency') }}