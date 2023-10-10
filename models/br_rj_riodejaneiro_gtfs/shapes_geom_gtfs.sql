{{ config(
       materialized = "incremental",
       partition_by = { "field" :"data",
       "data_type" :"date",
       "granularity": "day" },
       unique_key = ["shape_id", "data"],
       incremental_strategy = "insert_overwrite",
       alias = 'shapes_geom'
) }} 


WITH trips AS (
       SELECT trip_id,
              shape_id,
              route_id,
              DATA,
              FROM {{ ref('trips_gtfs') }} t
       WHERE data = "{{ var('data_versao_gtfs') }}"
),
contents AS (
       -- EXTRACTS VALUES FROM JSON STRING FIELD 'content'
       SELECT shape_id,
              ST_GEOGPOINT(shape_pt_lon, shape_pt_lat) ponto_shape,
              shape_pt_sequence,
              DATA,
              FROM {{ ref('shapes_gtfs') }} s
       WHERE data = "{{ var('data_versao_gtfs') }}"
),
pts AS (
       SELECT *,
              MAX(shape_pt_sequence) OVER(PARTITION BY DATA, shape_id) final_pt_sequence
       FROM contents c
       ORDER BY DATA,
              shape_id,
              shape_pt_sequence
),
shapes AS (
       -- BUILD LINESTRINGS OVER SHAPE POINTS
       SELECT shape_id,
              DATA,
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
       SELECT trip_id,
              m.shape_id,
              route_id,
              m.data,
              ROW_NUMBER() OVER(PARTITION BY m.data, m.shape_id, l.trip_id) rn
       FROM merged m
)
SELECT *
EXCEPT(rn)
FROM ids
WHERE rn = 1
