{{ config(
    materialized='incremental',
        partition_by={
        "field":"data",
        "data_type": "date",
        "granularity":"day"
    },
    unique_key=['data', 'trip_id'],
    incremental_strategy='insert_overwrite'
)
}}

-- 1. Puxa dados de distancia quadro no quadro horário
quadro as (
    select
        data AS UNNEST()
        e.tipo_dia,
        * except(tipo_dia, start_time, end_time),
        start_time as inicio_periodo,
        end_time as fim_periodo
    from (
        select * 
        from {{ var("quadro_horario") }}
        {% if is_incremental() %}
        where 
            timestamp_captura = date("{{ var("timestamp_captura_gtfs") }}")
        {% endif %}
    )
    on
        e.data_versao_frequencies = p.data_versao
    and
        e.tipo_dia = p.tipo_dia
),
-- 3. Trata informação de trips: adiciona ao sentido da trip o sentido
--    planejado (os shapes/trips circulares são separados em
--    ida/volta no sigmob)
trips as (
    select
        e.data,
        t.*
    from (
        select *
        from {{ ref('subsidio_trips_desaninhada') }}
        {% if is_incremental() %}
        where 
            data_versao = date("{{ var("shapes_version") }}")
        {% endif %}
    ) t
    inner join 
        data_efetiva e
    on 
        t.data_versao = e.data_versao_trips
),
quadro_trips as (
    select
        *
    from (
        select distinct
            * except(trip_id),
            trip_id as trip_id_planejado,
            trip_id
        from
            quadro
        where sentido = "I" or sentido = "V"
    )
    union all (
        select
            * except(trip_id),
            trip_id as trip_id_planejado,
            concat(SUBSTR(trip_id, 1, 10), "I", SUBSTR(trip_id, 12, length(trip_id))) as trip_id,
        from
            quadro
        where sentido = "C"
    )
    union all (
        select
            * except(trip_id),
            trip_id as trip_id_planejado,
            concat(SUBSTR(trip_id, 1, 10), "V", SUBSTR(trip_id, 12, length(trip_id))) as trip_id,
        from
            quadro
        where sentido = "C"
    )
),
quadro_tratada as (
    select
        q.*,
        t.shape_id,
        case 
            when sentido = "C"
            then concat(SUBSTR(shape_id, 1, 10), "C", SUBSTR(shape_id, 12, length(shape_id))) 
            else shape_id
        end as shape_id_planejado, -- TODO: adicionar no sigmob
    from
        quadro_trips q
    left join 
        trips t
    on 
        t.data = q.data
    and
        t.trip_id = q.trip_id
),
-- 4. Trata informações de shapes: junta trips e shapes para resgatar o sentido
--    planejado (os shapes/trips circulares são separados em
--    ida/volta no sigmob)
shapes as (
    select
        e.data,
        data_versao as data_shape,
        shape_id,
        shape,
        SUBSTR(shape_id, 11, 1) as sentido_shape,
        start_pt,
        end_pt
    from 
        data_efetiva e
    inner join (
        select * 
        from {{ ref('subsidio_shapes_geom') }}
        {% if is_incremental() %}
        where 
            data_versao = date("{{ var("shapes_version") }}")
        {% endif %}
    ) s
    on 
        s.data_versao = e.data_versao_shapes
)
-- 5. Junta shapes e trips aos servicos planejados no quadro horário
select 
    p.*,
    s.* except(data, shape_id)
from
    quadro_tratada p
inner join
    shapes s
on 
    p.shape_id = s.shape_id
and
    p.data = s.data