{{ config(
       partition_by = { 'field' :'data_versao',
       'data_type' :'date',
       'granularity': 'day' },
       unique_key = ['shape_id', 'data_versao'],
       alias = 'shapes_geom'
) }} 


WITH contents AS (
       SELECT shape_id,
              ST_GEOGPOINT(shape_pt_lon, shape_pt_lat) AS ponto_shape,
              shape_pt_sequence,
              data_versao,
              FROM {{ref('shapes_gtfs')}} s
       WHERE data_versao = '{{ var("data_versao_gtfs") }}'
),
pts AS (
       SELECT *,
              MAX(shape_pt_sequence) OVER(PARTITION BY data_versao, shape_id) final_pt_sequence
       FROM contents c
       ORDER BY data_versao,
              shape_id,
              shape_pt_sequence
),
shapes AS (
       -- BUILD LINESTRINGS OVER SHAPE POINTS
       SELECT shape_id,
              data_versao,
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
              c1.data_versao
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
              AND c1.data_versao = c2.data_versao
),
merged AS (
       -- JOIN SHAPES AND BOUNDARY POINTS
       SELECT s.*,
              b.*
       EXCEPT(data_versao, shape_id),
              ROUND(ST_LENGTH(shape), 1) shape_distance,
              FROM shapes s
              JOIN boundary b ON s.shape_id = b.shape_id
              AND s.data_versao = b.data_versao
),
ids AS (
       SELECT data_versao,
              shape_id,
              shape,
              shape_distance,
              start_pt,
              end_pt,
              ROW_NUMBER() OVER(PARTITION BY data_versao, shape_id) rn
       FROM merged m
)
SELECT 
       * EXCEPT(rn),
       '{{ var("version") }}' as versao_modelo
FROM ids
WHERE rn = 1
