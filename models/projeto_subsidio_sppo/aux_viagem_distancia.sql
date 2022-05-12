-- 1. Calcula a distância total percorrida por viagem
with distancia as (
    select distinct
        id_viagem,
        sum(distancia)/1000 distancia_aferida,
        sum(case when status_viagem = "middle" then 1 else 0 end) as n_registros_middle,
        sum(case when status_viagem = "start" then 1 else 0 end) as n_registros_start,
        sum(case when status_viagem = "end" then 1 else 0 end) as n_registros_end,
        sum(case when status_viagem = "out" then 1 else 0 end) as n_registros_out,
        count(timestamp_gps) as n_registros_total,
        count(distinct timestamp_minuto_gps) as n_registros_minuto
    from 
        {{ ref("aux_registros_status_viagem") }}
    group by 1
),
viagem as (
    select distinct
        id_viagem,
        servico_realizado,
        data
    from 
        {{ ref("aux_viagem_inicio_fim") }}
)
select distinct
    v.*,
    d.* except(id_viagem),
    n_registros_middle + n_registros_start + n_registros_end as n_registros_shape,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    viagem v
inner join 
    distancia d
on
    v.id_viagem = d.id_viagem