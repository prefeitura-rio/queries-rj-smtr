-- 1. Sumariza viagens aferidas
with viagem_paga as (
    select
        data,
        trip_id,
        count(id_viagem) as viagens_realizadas
    from 
        `rj-smtr.projeto_subsidio_sppo.viagem_completa` -- {{ ref("viagem_completa") }} -- todo: ref to prod
    group by
        1,2
),
viagem_recurso as (
    select
        data,
        trip_id,
        count(id_viagem) as viagens_realizadas
    from 
        {{ ref("viagem_completa_recurso") }}
    group by
        1,2
),
viagem as (
    select
        coalesce(p.data, r.data) as data,
        coalesce(p.trip_id, r.trip_id) as trip_id,
        (ifnull(p.viagens_realizadas,0) + ifnull(r.viagens_realizadas,0)) as viagens_realizadas
    from 
        viagem_paga p
    left join
        viagem_recurso r
    on
        r.trip_id = p.trip_id
        and r.data = p.data  
),
-- 2. Junta informações de viagens planejadas às realizadas
planejado as (
    select distinct
        p.*,
        ifnull(v.viagens_realizadas, 0) as viagens_realizadas,
        ifnull(v.viagens_realizadas, 0) as viagens_subsidio, -- sem limite maximo de viagens
    from (
        select
            consorcio,
            data,
            tipo_dia,
            trip_id_planejado as trip_id,
            servico,
            vista,
            sentido,
            inicio_periodo,
            fim_periodo,
            case
                when sentido = "C" then max(distancia_planejada)
                else sum(distancia_planejada) 
            end as distancia_planejada,
            max(distancia_total_planejada) as distancia_total_planejada, -- distancia total do dia (junta ida+volta)
            null as viagens_planejadas -- max(viagens) as viagens_planejadas
        from
            `rj-smtr.projeto_subsidio_sppo.viagem_planejada` -- {{ ref("viagem_planejada") }} # todo: ref to prod
        where data <= date_sub(current_date(), interval 1 day)
        group by 1,2,3,4,5,6,7,8,9
    ) p
    left join
        viagem v
    on
        v.trip_id = p.trip_id
        and v.data = p.data
)
-- 4. Adiciona informações de distância total
select 
    * except(distancia_planejada, distancia_total_planejada),
    distancia_total_planejada,
    round(viagens_subsidio * distancia_planejada, 3) as distancia_total_subsidio,
    round(viagens_realizadas * distancia_planejada, 3) as distancia_total_aferida,
    '{{ var("version") }}' as versao_modelo
from planejado