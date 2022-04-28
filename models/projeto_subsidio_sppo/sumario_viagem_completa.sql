-- 1. Filtra viagens completas apenas no período de operação da linha
with viagem_periodo as (
    select 
        v.*,
        p.start_time as inicio_periodo,
        p.end_time as fim_periodo,
        p.viagens as viagens_teorico
    from {{ ref("viagem_completa") }} v
    left join 
        {{ var("sppo_quadro_horario") }} p
    on 
        v.servico = p.servico
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
                or (extract (time from datetime_partida) <= end_time) -- apos 00h
            )
        )
    )
)
select 
    consorcio,
    data, 
    tipo_dia,
    servico,
    sentido,
    inicio_periodo, 
    fim_periodo,
    viagens_teorico,
    count(sentido) as viagens_realizadas,
    round(max(distancia_teorica)*viagens_teorico,2) as distancia_completa_teorica,
    round(max(distancia_teorica)*count(id_viagem),2) as distancia_realizada_teorica,
    round(sum(distancia_aferida),2) as distancia_realizada_aferida,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from viagem_periodo
group by 1,2,3,4,5,6,7,8