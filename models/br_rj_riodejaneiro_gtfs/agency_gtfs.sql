{ { config(
  materialized = "incremental",
  partition_by = { "field" :"data_versao_gtfs",
  "data_versao_gtfs_type" :"date",
  "granularity": "day" },
  unique_key = ["agency_id", "data_versao_gtfs"],
  incremental_strategy = "insert_overwrite",
  alias = 'agency',
) } } 

WITH t AS (
  SELECT SAFE_CAST(agency_id AS STRING) agency_id,
    REPLACE(content, "None", "") content,
    SAFE_CAST(data_versao_gtfs AS DATE) data_versao_gtfs
  FROM { { source('br_rj_riodejaneiro_gtfs_staging', 'agency') } }
)

SELECT agency_id,
  JSON_VALUE(content, "$.agency_name") agency_name,
  JSON_VALUE(content, "$.agency_url") agency_url,
  JSON_VALUE(content, "$.agency_timezone") agency_timezone,
  JSON_VALUE(content, "$.agency_lang") agency_lang,
  DATE(data_versao_gtfs) data_versao_gtfs
FROM t
