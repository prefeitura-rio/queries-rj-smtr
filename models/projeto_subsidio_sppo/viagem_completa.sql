-- Todas as viagens classificadas
-- tipo_viagem: 
-- - "Completa linha correta" => circular, nao_circular OK
-- - "Completa linha incorreta" => X não entra ainda

-- 1. Identifica viagens que estão dentro do quadro planejado
with viagem_periodo as (
    select distinct
        p.consorcio,
        v.*,
        p.inicio_periodo,
        p.fim_periodo,
        p.tempo_viagem as tempo_planejado
    from 
        {{ ref("viagem_planejada") }} p
    left join 
        {{ ref("viagem_conformidade") }} v        
    on 
        -- TODO: mudar servico & tipo dia => trip_id com mudança do quadro planejado
        v.servico_realizado = p.servico
        and v.tipo_dia = p.tipo_dia
        and v.sentido = p.sentido
        and v.data = p.data
    where (
        ( -- 05:00:00 as 23:00:00
            inicio_periodo < time_sub(fim_periodo, interval p.intervalo minute) 
            and extract (time from datetime_partida) >= inicio_periodo 
                and extract (time from datetime_partida) < time_sub(fim_periodo, interval p.intervalo minute)
        ) or
        ( -- 23:00:00 as 5:00:00
            inicio_periodo > time_sub(fim_periodo, interval intervalo minute)
            and ((extract (time from datetime_partida) >= inicio_periodo) -- até 00h
                or (extract (time from datetime_partida) < time_sub(fim_periodo, interval p.intervalo minute)) -- apos 00h
            )
        )
    )
)
-- 2. Seleciona viagens completas de acordo com a conformidade
select distinct
    consorcio,
    data,
    tipo_dia,
    id_empresa,
    id_veiculo,
    id_viagem,
    servico_informado,
    servico_realizado,
    shape_id,
    sentido,
    datetime_partida,
    datetime_chegada,
    inicio_periodo,
    fim_periodo,
    case 
        when servico_realizado = servico_informado
        then "Completa linha correta"
        else "Completa linha incorreta"
        end as tipo_viagem,
    tempo_viagem,
    tempo_planejado,
    distancia_planejada,
    distancia_aferida,
    n_registros_shape,
    n_registros_total,
    n_registros_minuto,
    perc_conformidade_shape,
    perc_conformidade_distancia,
    perc_conformidade_registros,
    round(100 * tempo_viagem/tempo_planejado, 2) as perc_conformidade_tempo,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    viagem_periodo v
where (
    perc_conformidade_distancia >= {{ var("perc_conformidade_distancia_min") }}
    )
and (
    perc_conformidade_shape >= {{ var("perc_conformidade_shape_min") }}
)
and (
    perc_conformidade_registros >= {{ var("perc_conformidade_registros_min") }}
)