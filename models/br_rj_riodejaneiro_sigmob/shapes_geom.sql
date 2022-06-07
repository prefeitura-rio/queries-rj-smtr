{{ config(
       materialized='incremental',
       partition_by={
              "field":"data_versao",
              "data_type": "date",
              "granularity":"day"
       }
)
}}
{% if is_incremental() and execute %}
       {% set start_date = run_query("select max(data_versao) from " ~ this).columns[0].values()[0] %}
{% else %}
       {% set start_date = "2021-08-24" %}
{% endif %}
with
       trips as (
              SELECT 
                     trip_id,
                     shape_id,
                     route_id,
                     DATE(data_versao) data_versao
              FROM {{ ref('trips_desaninhada') }} t
              WHERE DATE(data_versao) between DATE("{{start_date}}") and DATE_ADD(DATE("{{start_date}}"), INTERVAL 15 DAY)
       ),
       linhas as (
              SELECT 
                     trip_id, 
                     shape_id,
                     t.route_id, 
                     route_short_name linha, 
                     idModalSmtr id_modal_smtr,
                     t.data_versao,
              FROM trips t
              INNER JOIN (
              SELECT *
              FROM {{ ref('routes_desaninhada') }}
              WHERE DATE(data_versao) between DATE("{{start_date}}") and DATE_ADD(DATE("{{start_date}}"), INTERVAL 15 DAY)
              ) r
              on t.route_id = r.route_id 
              and t.data_versao = r.data_versao
       ),
       contents as (
       -- EXTRACTS VALUES FROM JSON STRING FIELD 'content' 
              SELECT 
                     shape_id,
                     ST_GEOGPOINT(
                            SAFE_CAST(json_value(content, "$.shape_pt_lon") AS FLOAT64),
                            SAFE_CAST(json_value(content, "$.shape_pt_lat") AS FLOAT64)
                     ) ponto_shape,
                     SAFE_CAST(json_value(content, "$.shape_pt_sequence") as INT64) shape_pt_sequence,
                     DATE(data_versao) AS data_versao
              FROM {{ ref('shapes') }} s
              WHERE DATE(data_versao) between DATE("{{start_date}}") and DATE_ADD(DATE("{{start_date}}"), INTERVAL 15 DAY)
       ),
       pts as (
              select 
                     *, 
                     max(shape_pt_sequence) over(
                            partition by data_versao, shape_id
                     ) final_pt_sequence
              from contents c
              order by data_versao, shape_id, shape_pt_sequence
       ),
       shapes as (
              -- BUILD LINESTRINGS OVER SHAPE POINTS
              SELECT 
                     shape_id, 
                     data_versao,
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
                     c1.data_versao
              FROM (select * from pts where shape_pt_sequence = 1) c1
              JOIN (select * from pts where shape_pt_sequence = final_pt_sequence) c2
              ON c1.shape_id = c2.shape_id and c1.data_versao = c2.data_versao
       ),
       merged as (
              -- JOIN SHAPES AND BOUNDARY POINTS
              SELECT 
                     s.*,
                     b.* except(data_versao, shape_id),
                     round(ST_LENGTH(shape),1) shape_distance,
              FROM shapes s
              JOIN boundary b
              ON s.shape_id = b.shape_id and s.data_versao = b.data_versao
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
                     m.data_versao,
                     row_number() over(
                            partition by m.data_versao, m.shape_id, l.trip_id
                     ) rn
              FROM merged m 
              JOIN linhas l
              ON m.shape_id = l.shape_id
              AND m.data_versao = l.data_versao
              -- mudar join para o route_id em todas as dependencias
       )
SELECT
       * except(rn)
FROM ids
WHERE rn = 1
{% if is_incremental %}
AND data_versao > DATE("{{start_date}}")
{% endif %}
