{{ config(
       materialized='incremental',
       partition_by={
              "field":"data",
              "data_type": "date",
              "granularity":"day"
       },
       unique_key=['shape_id'],
       incremental_strategy='insert_overwrite'
)
}}

-- 1. Geolocaliza os pontos da sequência de cada shape
with contents as (
    SELECT 
        shape_id,
        ST_GEOGPOINT(
            SAFE_CAST(shape_pt_lon AS FLOAT64),
            SAFE_CAST(shape_pt_lat AS FLOAT64)
        ) shape_pt_geo,
        SAFE_CAST(shape_pt_sequence as INT64) shape_pt_sequence,
        timestamp_captura,
        data,
        hora
    FROM 
        {{ ref("shapes") }} s
    WHERE 
      timestamp_captura = DATETIME("{{ var("gtfs_version") }}")
      and data = DATE("{{ var("gtfs_version") }}")
      and shape_id in ("O0432AAA0AIDU02", "O0010AAA0ACDU02") -- TODO: consertar shapes bugados
),
-- 2. Identifica pontos inicial, final e médio dos shapes
start_pt as (
    select
      shape_id, 
      shape_pt_geo as shape_start_pt,
      shape_pt_sequence
    from
      contents c
    where shape_pt_sequence = 1
),
end_pt as (
    select
      shape_id,
      shape_pt_geo as shape_end_pt,
      shape_pt_sequence as shape_end_sequence
    from contents c
    WHERE TRUE
    QUALIFY row_number() over(partition by shape_id order by shape_pt_sequence desc) = 1   
),
middle_pt as (
    select
      c.shape_id,
      c.shape_pt_geo as shape_middle_pt,
      c.shape_pt_sequence as shape_middle_sequence
    from contents c
    join end_pt e
    on e.shape_id = c.shape_id
    and c.shape_pt_sequence = FLOOR(shape_end_sequence/2)
),
-- 3. Identifica e quebra shapes circulares no ponto médio
aux_loops as (
  select
    s.shape_id
  from start_pt s
  inner join end_pt e
  on s.shape_id = e.shape_id
  and ST_DISTANCE(s.shape_start_pt, e.shape_end_pt) < 165 -- max: 160
),
loops as (
  select
    concat(c.shape_id, "_0") AS shape_id,
    c.* except(shape_id)
  from contents c
  join aux_loops a
  on 
    c.shape_id = a.shape_id
  join middle_pt m
  on 
    c.shape_id = a.shape_id
    and shape_pt_sequence <= m.shape_middle_sequence
  union all (
    select
      concat(c.shape_id, "_1") AS shape_id,
      c.* except(shape_id)
    from contents c
    join aux_loops a
    on 
      c.shape_id = a.shape_id
    join middle_pt m
    on 
      c.shape_id = m.shape_id
      and shape_pt_sequence >= m.shape_middle_sequence
  )
),
-- 3. Constrói a linestring de cada shape circular
loop_shapes as (
  SELECT
    * except(shape_pt_geo),
    st_makeline(ARRAY_AGG(shape_pt_geo)) as shape,
    FROM (
      select
        timestamp_captura, shape_id, shape_pt_geo
      from loops
      order by shape_id, shape_pt_sequence
  )
  group by 1,2
),
shapes as (
  SELECT
    * except(shape_pt_geo),
    st_makeline(ARRAY_AGG(shape_pt_geo)) as shape,
    FROM (
      select
        timestamp_captura, shape_id, shape_pt_geo
      from contents
      where shape_id not in (select shape_id from aux_loops)
      order by shape_id, shape_pt_sequence
  )
  group by 1,2
)
-- 4. Calcula distância do shape
select
  extract(date from timestamp_captura) as data,
  extract(hour from timestamp_captura) as hora,
  shape_id,
  shape,
  ST_STARTPOINT(shape) as shape_start_pt,
  ST_ENDPOINT(shape) as shape_end_pt,
  ROUND(ST_LENGTH(shape)/1000, 3) as shape_distance,
  timestamp_captura
FROM ((
  select * from shapes
)
union all (
  select * from loop_shapes
))