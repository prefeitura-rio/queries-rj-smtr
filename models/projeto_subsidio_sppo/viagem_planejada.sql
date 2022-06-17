{{ config(
       materialized = 'incremental',
       partition_by = {
              "field": "data",
              "data_type": "date",
              "granularity": "day"
       }
)
}}

-- 1. Define a data e tipo dia do período avaliado
with data_efetiva as (
    select data, tipo_dia
    from {{ ref("subsidio_data_versao_efetiva") }}
    where
        data = date_sub(date("{{ var("run_date") }}"), interval 2 day)
),
planejada as (
    select
        e.*,
        v.* except(tipo_dia)
    from 
        data_efetiva e
    left join
        {{ var("quadro_horario") }} v
    on
        e.tipo_dia = v.tipo_dia
),
-- 3. Adiciona informações do trajeto (shape) - ajusta distancia
--    planejada total de viagens circulares (ida+volta)
distancia as (
    select
        data,
        tipo_dia,
        servico,
        sentido,
        data_versao as data_shape,
        shape_id,
        round(distancia_planejada, 3) as distancia_planejada,   
    from (
        select 
            *
        from (
            select
                * except(distancia_planejada, shape_id),
                case 
                    when sentido = "C"
                    then concat(SUBSTR(shape_id, 1, 10), "C", SUBSTR(shape_id, 12, length(shape_id)))
                    else shape_id
                end as shape_id,
                case
                    when sentido = "C"
                    then distancia_planejada + lead(distancia_planejada) over (
                            partition by data, servico, tipo_dia --, variacao_itinerario
                            order by data, servico, sentido_shape)
                    else distancia_planejada
                end as distancia_planejada
            from
                {{ ref("aux_shapes_filtrada") }} v
        )
    where
        (sentido = "I" or sentido = "V")
        or (sentido = "C" and sentido_shape = "I")
    )
)
-- 2. Junta consórcios e distancia shape aos servicos planejados
select 
    p.*,
    d.* except(data, servico, sentido, tipo_dia)
from
    planejada p
left join 
    distancia d
on p.data = d.data
and p.servico = d.servico
and p.sentido = d.sentido
and p.tipo_dia = d.tipo_dia