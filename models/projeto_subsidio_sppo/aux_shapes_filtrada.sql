-- 1. Define a data e tipo dia do período avaliado (D-2, D-1)
with data_efetiva as (
    select 
        *
    from {{ ref("subsidio_data_versao_efetiva") }}
    where
        data between date_sub(date("{{ var("run_date") }}"), interval 2 day)
            and date_sub(date("{{ var("run_date") }}"), interval 1 day)
),
-- 2. Puxa versao fixa de shapes do sigmob
shapes as (
    select
        data_versao,
        trip_short_name as servico,
        trip_id,
        shape_id,
        -- SUBSTR(shape_id, 12, 2) as variacao_itinerario,
        SUBSTR(shape_id, 11, 1) as sentido_shape,
        shape,
        round(s.shape_distance/1000, 3) as distancia_planejada,
        start_pt,
        end_pt
    from {{ ref('subsidio_shapes_geom') }} s
    where
        data_versao in (
            select 
                distinct data_versao_sigmob
            from 
                data_efetiva)
    --     and id_modal_smtr in ('22','O')
),
-- 3. Adiciona data atual dos shapes
shapes_efetiva as (
    select 
        e.data,
        e.tipo_dia,
        s.*
    from 
        data_efetiva e
    left join
        shapes s
    on
        s.data_versao = e.data_versao_sigmob
),
-- 4. Filtra shapes de servicos circulares planejados (recupera
--    sentido dos shapes separados em ida/volta)
shape_circular as (
    select distinct
        s.shape_id,
        c.sentido
    from 
        shapes_efetiva s
    inner join (
        select
            *
        from 
            {{ var("quadro_horario") }}
        where
            sentido = "C"
    ) c
    on
        s.servico = c.servico
        and s.tipo_dia = c.tipo_dia
        -- and s.variacao_itinerario = c.variacao_itinerario
),
-- 5. Filtra shapes de servicos não circulares planejados
shape_nao_circular as (
    select distinct
        s.shape_id,
        c.sentido
    from 
        shapes_efetiva s
    inner join (
        select 
            *
        from 
            {{ var("quadro_horario") }}
        where 
            sentido = "I" or sentido = "V"
    ) c
    on 
        s.trip_id = c.trip_id
),
-- 6. Junta infos de shapes circulares e não ciculares
shape_sentido as (
    select 
        * 
    from 
        shape_circular
    union all  (
        select 
            *
        from 
            shape_nao_circular
    )
)
select
    e.*,
    s.sentido,
    '{{ var("version") }}' as versao_modelo
from 
    shapes_efetiva e
inner join 
    shape_sentido s
on 
    e.shape_id = s.shape_id