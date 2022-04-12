SELECT
  stop_id,
  JSON_VALUE(content, "$.trip_id") trip_id,
  JSON_VALUE(content, "$.arrival_time") arrival_time,
  JSON_VALUE(content, "$.departure_time") departure_time,
  JSON_VALUE(content, "$.stop_sequence") stop_sequence,
  JSON_VALUE(content, "$.shape_dist_traveled") shape_dist_traveled,
  DATE(data_versao) data_versao
FROM {{ ref('stop_times') }}