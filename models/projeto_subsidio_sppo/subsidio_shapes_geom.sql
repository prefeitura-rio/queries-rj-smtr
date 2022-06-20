{{ config(
    materialized='incremental',
        partition_by={
        "field":"data_versao",
        "data_type": "date",
        "granularity":"day"
    }
)
}}

-- ATUALIZADA A CADA 15 DIAS
with trips as (
    SELECT 
        trip_id,
        trip_id_planejado,
        shape_id,
        shape_id_planejado,
        route_id,
        trip_short_name,
        sentido
    FROM {{ ref("subsidio_trips_desaninhada") }}
    -- TODO: remover quando consertar o shape
    where shape_id not in ("O0410AAA0AIDU01", "O0410AAA0AVDU01")
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
    FROM 
        `rj-smtr.br_rj_riodejaneiro_sigmob.shapes` -- {{ ref("shapes") }} s
    WHERE 
        DATE(data_versao) = "{{var('versao_fixa_sigmob')}}"
        -- TODO: remover quando consertar o shape
        and shape_id not in ("O0410AAA0AIDU01", "O0410AAA0AVDU01")
),
pts as (
    select
        *, 
        max(shape_pt_sequence) over(
                partition by data_versao, shape_id
        ) final_pt_sequence
    from 
        contents c
    order by 
        data_versao, shape_id, shape_pt_sequence
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
    FROM 
        (select * from pts where shape_pt_sequence = 1) c1
    JOIN 
        (select * from pts where shape_pt_sequence = final_pt_sequence) c2
    ON 
        c1.shape_id = c2.shape_id and c1.data_versao = c2.data_versao
),
merged as (
-- JOIN SHAPES AND BOUNDARY POINTS
    SELECT 
        s.*,
        b.* except(data_versao, shape_id),
        round(ST_LENGTH(shape),1) shape_distance,
    FROM 
        shapes s
    JOIN 
        boundary b
    ON 
        s.shape_id = b.shape_id and s.data_versao = b.data_versao
),
ids as (
    SELECT 
        t.trip_id,
        t.trip_id_planejado,
        t.shape_id,
        t.shape_id_planejado,
        t.trip_short_name,
        t.sentido,
        SUBSTR(t.shape_id, 11, 1) as sentido_shape,
        shape,
        shape_distance,
        start_pt,
        end_pt,
        data_versao,
        row_number() over(
                partition by data_versao, t.shape_id, trip_id
        ) rn
    FROM merged m
    inner join trips t
    on t.shape_id = m.shape_id
)
SELECT
       * except(rn),
       "{{ var("version") }}" as versao_modelo
FROM ids
WHERE rn = 1