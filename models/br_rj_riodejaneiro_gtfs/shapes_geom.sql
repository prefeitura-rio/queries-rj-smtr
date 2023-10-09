{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data_versao_gtfs",
      "data_versao_gtfs_type":"date",
      "granularity": "day"
    },
    unique_key=["shape_id", "data_versao_gtfs"],
    incremental_strategy="insert_overwrite",
    alias='shapes_geom',
  )
}}

with
       trips as (
              SELECT 
                     trip_id,
                     shape_id,
                     route_id,
                     data_versao_gtfs
              FROM {{ ref('trips') }} t
              WHERE data_versao_gtfs = {{var('data_versao_gtfs_gtfs')}} 
       ),
       contents as (
       -- EXTRACTS VALUES FROM JSON STRING FIELD 'content' 
              SELECT 
                     shape_id,
                     ST_GEOGPOINT(
                           shape_pt_lon,
                           shape_pt_lat
                     ) ponto_shape,
                     shape_pt_sequence,
                     data_versao_gtfs,
              FROM {{ ref('shapes') }} s
              WHERE data_versao_gtfs = {{var('data_versao_gtfs_gtfs')}}
       ),
       pts as (
              select 
                     *, 
                     max(shape_pt_sequence) over(
                            partition by data_versao_gtfs, shape_id
                     ) final_pt_sequence
              from contents c
              order by data_versao_gtfs, shape_id, shape_pt_sequence
       ),
       shapes as (
              -- BUILD LINESTRINGS OVER SHAPE POINTS
              SELECT 
                     shape_id, 
                     data_versao_gtfs,
                     st_makeline(ARRAY_AGG(ponto_shape)) as shape
              FROM pts
              GROUP BY 1,2
       ),
       boundary as (
              -- EXTRACT START AND END POINTS FROM SHAPES
              SELECT 
                     c1.shape_id,
                     c1.ponto_shape start_pt,
                     c2.ponto_shape end_pt,
                     c1.data_versao_gtfs
              FROM (select * from pts where shape_pt_sequence = 1) c1
              JOIN (select * from pts where shape_pt_sequence = final_pt_sequence) c2
              ON c1.shape_id = c2.shape_id and c1.data_versao_gtfs = c2.data_versao_gtfs
       ),
       merged as (
              -- JOIN SHAPES AND BOUNDARY POINTS
              SELECT 
                     s.*,
                     b.* except(data_versao_gtfs, shape_id),
                     round(ST_LENGTH(shape),1) shape_distance,
              FROM shapes s
              JOIN boundary b
              ON s.shape_id = b.shape_id and s.data_versao_gtfs = b.data_versao_gtfs
       ),
       ids as (
              SELECT 
                     trip_id,
                     m.shape_id,
                     route_id,
                     id_modal_smtr,
                     replace(linha, " ", "") as linha_gtfs, 
                     shape,
                     shape_distance, 
                     start_pt, 
                     end_pt,
                     m.data_versao_gtfs,
                     row_number() over(
                            partition by m.data_versao_gtfs, m.shape_id, l.trip_id
                     ) rn
              FROM merged m 
              JOIN linhas l
              ON m.shape_id = l.shape_id
              AND m.data_versao_gtfs = l.data_versao_gtfs
              -- mudar join para o route_id em todas as dependencias
       )
SELECT
       * except(rn)
FROM ids
WHERE rn = 1

