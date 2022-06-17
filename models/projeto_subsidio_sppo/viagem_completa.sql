-- Todas as viagens classificadas
-- tipo_viagem: 
-- - "Completa linha correta" => circular, nao_circular OK
-- - "Completa linha incorreta" => X não entra ainda

-- 1. Identifica viagens que estão dentro do quadro planejado (por
--    enquanto, consideramos o dia todo).
with viagem_periodo as (
    select distinct
        p.consorcio,
        p.vista,
        v.*,
        time("2022-06-01 00:00:00") as inicio_periodo,
        time("2022-06-01 23:59:59") as fim_periodo,
        0 as tempo_planejado
    from (
        select distinct
            consorcio,
            vista,
            data,
            shape_id,
            servico
        from
            {{ ref("viagem_planejada") }}
    ) p
    left join 
        {{ ref("viagem_conformidade") }} v 
    on 
        v.shape_id = p.shape_id
        and v.servico_informado = p.servico
        and v.data = p.data
    -- where (
    --     ( -- 05:00:00 as 23:00:00
    --         inicio_periodo < time_sub(fim_periodo, interval p.intervalo minute) 
    --         and extract (time from datetime_partida) >= inicio_periodo 
    --             and extract (time from datetime_partida) < time_sub(fim_periodo, interval p.intervalo minute)
    --     ) or
    --     ( -- 23:00:00 as 5:00:00
    --         inicio_periodo > time_sub(fim_periodo, interval intervalo minute)
    --         and ((extract (time from datetime_partida) >= inicio_periodo) -- até 00h
    --             or (extract (time from datetime_partida) < time_sub(fim_periodo, interval p.intervalo minute)) -- apos 00h
    --         )
    --     )
    -- )
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
    vista,
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
    0 as perc_conformidade_tempo,
    -- round(100 * tempo_viagem/tempo_planejado, 2) as perc_conformidade_tempo,
    '{{ var("version") }}' as versao_modelo
from 
    viagem_periodo v
where (
    perc_conformidade_shape >= {{ var("perc_conformidade_shape_min") }}
)
and (
    perc_conformidade_distancia >= {{ var("perc_conformidade_distancia_min") }}
)
and (
    perc_conformidade_registros >= {{ var("perc_conformidade_registros_min") }}
)