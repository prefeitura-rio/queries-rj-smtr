{{ config(
       materialized='incremental',
       unique_key=['shape_id', 'data_versao']
       )
}}
with
       trips as (
              SELECT 
                     trip_id,
                     route_id,
                     DATE(data_versao) data_versao
              FROM {{ ref('trips_desaninhada') }} t
              WHERE DATE(t.data_versao) between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
       ),
       linhas as (
              SELECT 
                     trip_id, t.route_id, 
                     route_short_name linha, 
                     idModalSmtr id_modal_smtr,
                     t.data_versao,
              FROM trips t
              INNER JOIN (
              SELECT *
              FROM {{ ref('routes_desaninhada') }}
              WHERE DATE(data_versao) between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
              ) r
              on t.route_id = r.route_id and t.data_versao = r.data_versao
       ),
       contents as (
       -- EXTRACTS VALUES FROM JSON STRING FIELD 'content' 
              SELECT shape_id,
              SAFE_CAST(json_value(content, "$.shape_pt_lat") AS FLOAT64) shape_pt_lat,
              SAFE_CAST(json_value(content, "$.shape_pt_lon") AS FLOAT64) shape_pt_lon,
              SAFE_CAST(json_value(content, "$.shape_pt_sequence") as INT64) shape_pt_sequence,
              DATE(data_versao) AS data_versao,
              FROM {{ ref('shapes') }} s
              WHERE DATE(data_versao) between DATE({{ date_range_start }}) and DATE({{ date_range_end }})
       ),
       pts as (
              -- CONSTRUCT POINT GEOGRAPHIES 
              SELECT * except(shape_pt_lon, shape_pt_lat), 
              st_geogpoint(shape_pt_lon, shape_pt_lat) as ponto_shape,
              row_number() over (partition by data_versao, shape_id order by shape_pt_sequence DESC) rn
              FROM contents
              ORDER BY data_versao, shape_id, shape_pt_sequence
       ),
       shapes as (
              -- BUILD LINESTRINGS OVER SHAPE POINTS
              SELECT 
                     shape_id, 
                     data_versao,
                     st_makeline(ARRAY_AGG(ponto_shape)) as shape
              FROM pts
              GROUP BY data_versao, shape_id
       ),
       boundary as (
              -- EXTRACT START AND END POINTS FROM SHAPES
              SELECT c1.shape_id,
                     c1.ponto_shape start_pt,
                     c2.ponto_shape end_pt,
                     c1.data_versao
              FROM (select * from pts where shape_pt_sequence = 1) c1
              JOIN (select * from pts where rn = 1) c2
              ON c1.shape_id = c2.shape_id and c1.data_versao = c2.data_versao
       ),
       merged as (
              -- JOIN SHAPES AND BOUNDARY POINTS
              SELECT s.shape_id, shape, 
                     round(ST_LENGTH(shape),1) shape_distance,
                     start_pt,
                     end_pt,
                     s.data_versao
              FROM shapes s
              JOIN boundary b
              ON s.shape_id = b.shape_id and s.data_versao = b.data_versao
       )
SELECT 
       trip_id,
       shape_id,
       route_id,
       id_modal_smtr,
       replace(linha, " ", "") as linha_gtfs, 
       shape,
       shape_distance, 
       start_pt, 
       end_pt,
       m.data_versao
FROM merged m 
JOIN linhas l
ON m.shape_id = l.trip_id
AND m.data_versao = l.data_versao

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where DATE(data_versao) > (select max(DATE(data_versao)) from {{ this }})

{% endif %}