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
        data_versao_efetiva_agency,
        case
            when extract(dayofweek from data) = 1 then 'Domingo'
            when extract(dayofweek from data) = 7 then 'Sabado'
            else 'Dia Útil'
        end as tipo_dia
    from 
        {{ var('sigmob_data_versao') }} a
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
        {{ ref("aux_viagem_planejada_tratada") }} v
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
        {{ var("sigmob_routes") }} a
    on
        a.data_versao = p.data_versao_efetiva_agency
        and p.servico = REPLACE(a.route_short_name, " ", "")
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
        distancia_planejada
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
-- 2. Junta consórcios e distancia shape aos servicos planejados
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
-- {% if is_incremental() %}
--     {% set max_partition = run_query("SELECT gr FROM (SELECT IF(max(data) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(data)) as gr FROM " ~ this ~ ")").columns[0].values()[0] %}
--     where p.data > DATE("{{max_partition}}")
-- {% endif %}