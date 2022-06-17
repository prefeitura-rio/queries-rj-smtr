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
    select distinct
        data,
        date("{{ var("subsidio_sigmob_date")}}") as data_versao_efetiva_agency,
        case
            when extract(dayofweek from data) = 1 then 'Domingo'
            when extract(dayofweek from data) = 7 then 'Sabado'
            else 'Dia Útil'
        end as tipo_dia
    from 
        {{ ref('data_versao_efetiva') }} a
    where
        data = date_sub(date("{{ var("run_date") }}"), interval 2 day)
),
planejada as (
    select
        e.*,
        start_time as inicio_periodo,
        end_time as fim_periodo,
        v.* except(tipo_dia, start_time, end_time)
    from 
        data_efetiva e
    left join
        {{ var("quadro_horario_dia_util") }} v
    on
        e.tipo_dia = v.tipo_dia
),
-- 2. Adiciona informação de consórcios dos serviços planejados
planejada_agency as (
    select
        a.agency_name as consorcio, 
        p.* except(data_versao_efetiva_agency),
    from 
        planejada p
    left join
        {{ ref("routes_desaninhada") }} a
    on
        a.data_versao = p.data_versao_efetiva_agency
        and and p.servico = a.servico
    where 
        a.idModalSmtr in ("22", "O")
),
-- 3. Adiciona informações do trajeto (shape) - ajusta distancia
--    planejada total de viagens circulares (ida+volta)
distancia as (
    select
        data,
        servico,
        sentido,
        variacao_itinerario,
        data_shape,
        shape_id,
        round(distancia_planejada, 3) as distancia_planejada,   
    from (
        select
            * except(distancia_planejada),
            case when
                sentido = "C" and sentido_shape = "I"
                then distancia_planejada + lead(distancia_planejada) over (
                        partition by data, servico, variacao_itinerario 
                        order by data, servico, variacao_itinerario, sentido_shape)
                else distancia_planejada
            end as distancia_planejada
        from
            {{ ref("aux_shapes_filtrada") }} v
    )
    where
        (sentido = "I" or sentido = "V")
        or (sentido = "C" and sentido_shape = "I")
)
-- 2. Junta consórcios e distancia shape aos servicos planejados,
--    padroniza servico como variação+linha (semelhante ao gps)
select 
    p.*,
    d.* except(data, servico, sentido, variacao_itinerario)
from
    planejada_agency p
left join 
    distancia d
on p.data = d.data
and p.servico = d.servico
and p.sentido = d.sentido
and p.variacao_itinerario = d.variacao_itinerario