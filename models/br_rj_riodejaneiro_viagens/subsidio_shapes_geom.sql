{{ config(
    materialized='incremental',
        partition_by={
        "field":"data_versao",
        "data_type": "date",
        "granularity":"day"
    },
    unique_key=['data_versao', 'shape_id'],
    incremental_strategy='insert_overwrite'
)
}}

with data_versao as (
    select data_versao_shapes
    from {{ ref("subsidio_data_versao_efetiva") }}
    where data between date_sub("{{ var("run_date") }}", interval 1 day) and date("{{ var("run_date") }}")
),
contents as (
    SELECT 
        shape_id,
        ST_GEOGPOINT(
            SAFE_CAST(shape_pt_lon AS FLOAT64),
            SAFE_CAST(shape_pt_lat AS FLOAT64)
        ) ponto_shape,
        SAFE_CAST(shape_pt_sequence as INT64) shape_pt_sequence,
        DATE(data_versao) AS data_versao
    FROM 
        {{ var("subsidio_shapes") }} s
    {% if is_incremental() %}
    WHERE
        data_versao in (select data_versao_shapes from data_versao)
    {% endif %}
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
        shape_id,
        shape,
        shape_distance,
        start_pt,
        end_pt,
        data_versao,
        row_number() over(
                partition by data_versao, shape_id
        ) rn
    FROM merged m
)
SELECT
       * except(rn),
       "{{ var("version") }}" as versao_modelo
FROM ids
WHERE rn = 1