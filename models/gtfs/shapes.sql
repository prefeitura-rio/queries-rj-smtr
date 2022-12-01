SELECT
  SAFE_CAST(DATA AS DATE) DATA,
  SAFE_CAST(hora AS INT64) hora,
  SAFE_CAST(shape_id AS STRING) shape_id,
  SAFE_CAST(shape_pt_lat AS FLOAT64) shape_pt_lat,
  SAFE_CAST(shape_pt_lon AS FLOAT64) shape_pt_lon,
  SAFE_CAST(shape_pt_sequence AS INT64) shape_pt_sequence,
  SAFE_CAST(shape_dist_traveled AS FLOAT64) shape_dist_traveled,
  SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura
FROM
  {{ var("shapes_staging") }} AS t