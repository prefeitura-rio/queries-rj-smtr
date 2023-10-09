{{ config(
  materialized = "incremental",
  partition_by = { "field" :"data",
  "data_type" :"date",
  "granularity": "day" },
  unique_key = ["agency_id", "data"],
  incremental_strategy = "insert_overwrite",
  alias = 'agency',
) }} 

SELECT SAFE_CAST(agency_id AS STRING) agency_id,
  SAFE_CAST(data AS DATE) data,
  SAFE_CAST(JSON_VALUE(content, "$.agency_name") AS STRING) agency_name,
  SAFE_CAST(JSON_VALUE(content, "$.agency_url") AS STRING) agency_url,
  SAFE_CAST(JSON_VALUE(content, "$.agency_timezone") AS STRING) agency_timezone,
  SAFE_CAST(JSON_VALUE(content, "$.agency_lang") AS STRING) agency_lang,
  FROM {{ source('br_rj_riodejaneiro_gtfs_staging', 'agency') }}
 WHERE data = "{{ var('data_versao_gtfs') }}"