-- 1. Adiciona período de operação da viagem
with viagem_periodo as (
    select 
        v.id_viagem,
        p.start_time as inicio_periodo,
        p.end_time as fim_periodo,
        p.tempo_viagem as tempo_planejado,
        p.viagens as viagens_planejadas
    from 
        {{ ref("aux_viagem_circular") }} v
    inner join 
        {{ var("sppo_viagem_planejada") }} p
    on 
        RTRIM(v.servico_realizado, " ") = RTRIM(p.servico, " ") -- ajusta tipo de servico entre tabelas (ex: 309 SN -> 309SN)
        and v.tipo_dia = p.tipo_dia
        and v.sentido = p.sentido
    where (
        ( -- 05:00:00 as 23:00:00
            start_time < end_time 
            and extract (time from datetime_partida) >= start_time 
                and extract (time from datetime_partida) < end_time
        ) or
        ( -- 23:00:00 as 5:00:00
            start_time > end_time 
            and ((extract (time from datetime_partida) >= start_time) -- até 00h
                or (extract (time from datetime_partida) < end_time) -- apos 00h
            )
        )
    )
),
viagem as (
    select
        v.* except(versao_modelo),
        p.* except(id_viagem)
    from 
        {{ ref("aux_viagem_circular") }} v
    left join 
        viagem_periodo p
    on 
        v.id_viagem = p.id_viagem
),
-- 2. Calcula a distância total percorrida por viagem
distancia as (
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
    group by 
        1,2
),
-- 3. Calcula os percentuais de conformidade da viagem
conformidade as (
    select 
        v.*,
        distancia_teorica,
        distancia_aferida,
        n_registros_middle,
        n_registros_start,
        n_registros_end,
        n_registros_out,
        n_registros_total,
        n_registros_minuto,
        n_registros_middle + n_registros_start + n_registros_end as n_registros_shape,
        round(100 * (n_registros_middle + n_registros_start + n_registros_end)/n_registros_total,2) as perc_conformidade_shape,
        round(100 * d.distancia_aferida/d.distancia_teorica, 2) as perc_conformidade_distancia,
        round(100 * n_registros_minuto/tempo_viagem, 2) as perc_conformidade_registros,
        round(100 * tempo_viagem/tempo_planejado, 2) as perc_conformidade_tempo
    from 
        viagem v -- {{ ref("aux_viagem_circular") }} v
    inner join 
        distancia d
    on
        v.id_viagem = d.id_viagem
)
select 
    c.*,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    conformidade c