{{config( MATERIALIZED = "incremental",
    partition_by = { "field" :"data",
    "data_type" :"date",
    "granularity": "day" },
    unique_key = ["trip_id",
    "data"],
    incremental_strategy = "insert_overwrite",
    alias = 'trips' )}}
WITH
  t AS (
  SELECT
    SAFE_CAST(trip_id AS STRING) trip_id,
    REPLACE(content, "None", "") content,
    SAFE_CAST(data AS DATE) data
  FROM
    {{ source('br_rj_riodejaneiro_gtfs_staging',
      'trips') }})
SELECT
  trip_id,
  JSON_VALUE(content, "$.route_id") route_id,
  JSON_VALUE(content, "$.service_id") service_id,
  JSON_VALUE(content, "$.trip_headsign") trip_headsign,
  JSON_VALUE(content, "$.trip_short_name") trip_short_name,
  JSON_VALUE(content, "$.direction_id") direction_id,
  JSON_VALUE(content, "$.shape_id") shape_id,
  DATE(data) data
FROM
  t