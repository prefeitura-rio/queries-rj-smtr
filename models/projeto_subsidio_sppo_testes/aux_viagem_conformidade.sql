-- 1. Calcula a distância total percorrida por viagem
with distancia as (
    select 
        id_viagem,
        distancia_teorica,
        round(sum(distancia)/1000, 2) distancia_aferida,
        sum(case when status_viagem = "middle" then 1 else 0 end) as n_registros_middle,
        sum(case when status_viagem = "start" then 1 else 0 end) as n_registros_start,
        sum(case when status_viagem = "end" then 1 else 0 end) as n_registros_end,
        sum(case when status_viagem = "out" then 1 else 0 end) as n_registros_out,
        count(timestamp_gps) as n_registros_total,
        count(distinct timestamp_minuto_gps) as n_registros_minuto
    from 
        {{ ref("aux_registros_status_viagem") }} 
    where 
        id_viagem is not null
    group by 
        1,2
),
-- 2. Calcula os percentuais de conformidade da viagem
conformidade as (
    select 
        v.* except(versao_modelo),
        distancia_teorica,
        distancia_aferida,
        n_registros_middle,
        n_registros_start,
        n_registros_end,
        n_registros_out,
        n_registros_total,
        n_registros_minuto,
        round(100 * (n_registros_middle + n_registros_start + n_registros_end)/n_registros_total,2) as perc_conformidade_shape,
        round(100 * d.distancia_aferida/d.distancia_teorica, 2) as perc_conformidade_distancia,
        round(100 * n_registros_minuto/tempo_viagem, 2) as perc_conformidade_registros,
        round(100 * tempo_viagem/tempo_planejado, 2) as perc_conformidade_tempo
    from 
        {{ ref("aux_viagem_periodo") }} v
    inner join 
        distancia d
    on
        v.id_viagem = d.id_viagem
    order by
        data, id_viagem
)
select 
    c.*,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    conformidade c