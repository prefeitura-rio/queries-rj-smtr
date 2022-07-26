{{ config(
    materialized='incremental',
        partition_by={
        "field":"data",
        "data_type": "date",
        "granularity":"day"
    }
)
}}

-- 1. Define datas do período planejado
with data_efetiva as (
    select 
        data,
        tipo_dia,
        data_versao_shapes,
        data_versao_trips,
        data_versao_frequencies
    from {{ ref("subsidio_data_versao_efetiva") }}
),
-- 2. Puxa dados de distancia quadro no quadro horário
quadro as (
    select
        e.data,
        e.tipo_dia,
        p.* except(tipo_dia, data_versao, horario_inicio, horario_fim),
        horario_inicio as inicio_periodo,
        horario_fim as fim_periodo
    from 
        data_efetiva e
    inner join
        {{ var("quadro_horario") }} p
    on
        e.data_versao_frequencies = p.data_versao
    and
        e.tipo_dia = p.tipo_dia
    {% if is_incremental() %}
    WHERE
        p.data_versao = date("{{ var("sigmob_version_date") }}")
    {% endif %}
),
-- 3. Trata informação de trips: adiciona ao sentido da trip o sentido
--    planejado (os shapes/trips circulares são separados em
--    ida/volta no sigmob)
trips as (
    select
        e.data,
        t.*
    from
        {{ ref('subsidio_trips_desaninhada') }} t
    inner join 
        data_efetiva e
    on 
        t.data_versao = e.data_versao_trips
    {% if is_incremental() %}
    WHERE
        t.data_versao = date("{{ var("sigmob_version_date") }}")
    {% endif %}
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
    inner join
        {{ ref('subsidio_shapes_geom') }} s
    on 
        s.data_versao = e.data_versao_shapes
    {% if is_incremental() %}
    WHERE
        s.data_versao = date("{{ var("sigmob_version_date") }}")
    {% endif %}
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