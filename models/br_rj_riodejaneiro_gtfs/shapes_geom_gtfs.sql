{{ config(
       materialized = "incremental",
       partition_by = { "field" :"data",
       "data_type" :"date",
       "granularity": "day" },
       unique_key = ["shape_id", "data"],
       incremental_strategy = "insert_overwrite",
       alias = "shapes_geom"
) }} 

WITH contents AS (
       -- EXTRACTS VALUES FROM JSON STRING FIELD 'content'
       SELECT shape_id,
       	      --CAST(shape_pt_lon AS FLOAT64) shape_pt_lon,
       	      --CAST(shape_pt_lat AS FLOAT64) shape_pt_lat,
              ST_GEOGPOINT(shape_pt_lon, shape_pt_lat) AS ponto_shape,
              shape_pt_sequence,
              data,
              FROM {{ref("shapes_gtfs")}} s
       WHERE data = "{{ var('data_versao_gtfs') }}"
),
pts AS (
       SELECT *,
              MAX(shape_pt_sequence) OVER(PARTITION BY data, shape_id) final_pt_sequence
       FROM contents c
       ORDER BY data,
              shape_id,
              shape_pt_sequence
),
shapes AS (
       -- BUILD LINESTRINGS OVER SHAPE POINTS
       SELECT shape_id,
              data,
              ST_MAKELINE(ARRAY_AGG(ponto_shape)) AS shape
       FROM pts
       GROUP BY 1,
              2
),
boundary AS (
       -- EXTRACT START AND END POINTS FROM SHAPES
       SELECT c1.shape_id,
              c1.ponto_shape start_pt,
              c2.ponto_shape end_pt,
              c1.data
       FROM (
                     SELECT *
                     FROM pts
                     WHERE shape_pt_sequence = 1
              ) c1
              JOIN (
                     SELECT *
                     FROM pts
                     WHERE shape_pt_sequence = final_pt_sequence
              ) c2 ON c1.shape_id = c2.shape_id
              AND c1.data = c2.data
),
merged AS (
       -- JOIN SHAPES AND BOUNDARY POINTS
       SELECT s.*,
              b.*
       EXCEPT(DATA, shape_id),
              ROUND(ST_LENGTH(shape), 1) shape_distance,
              FROM shapes s
              JOIN boundary b ON s.shape_id = b.shape_id
              AND s.data = b.data
),
ids AS (
       SELECT m.shape_id,
              m.data,
              ROW_NUMBER() OVER(PARTITION BY m.data, m.shape_id) rn
       FROM merged m
)
SELECT *
EXCEPT(rn)
FROM ids
WHERE rn = 1
