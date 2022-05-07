with viagem_periodo as (
    select 
        v.id_viagem,
        p.start_time as inicio_periodo,
        p.end_time as fim_periodo,
        p.tempo_viagem as tempo_planejado
    from 
        {{ ref("aux_viagem_circular") }} v
    inner join 
        {{ var("sppo_viagem_planejada") }} p
    on 
        v.servico_realizado = p.servico
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
)
select
    v.*,
    p.* except(id_viagem)
from 
    {{ ref("aux_viagem_circular") }} v
left join 
    viagem_periodo p
on 
    v.id_viagem = p.id_viagem