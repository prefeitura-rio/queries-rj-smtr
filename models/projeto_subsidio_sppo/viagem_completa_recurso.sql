{{ 
config(
    materialized='incremental',
    partition_by={
            "field":"data",
            "data_type": "date",
            "granularity":"day"
    },
    unique_key="data",
    incremental_strategy = 'insert_overwrite'
)
}}

with recursos_viagem as (
    select distinct * 
    from {{ ref('viagem_conformidade_recurso') }}
    {% if is_incremental() %}
        where data between date('{{ var("recurso_viagem_start")}}') and date('{{ var("recurso_viagem_end")}}')
    {% endif %}
),
viagem_planejada as (
    select distinct
        consorcio,
        vista,
        data,
        tipo_dia,
        trip_id_planejado as trip_id,
        servico
    from
        `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada` -- todo: ref to prod
    {% if is_incremental() %}
        where data between date('{{ var("recurso_viagem_start")}}') and date('{{ var("recurso_viagem_end")}}')
    {% endif %}
),
-- 1. Identifica viagens que estão dentro do quadro planejado (por
--    enquanto, consideramos o dia todo).
viagem_periodo as (
    select distinct
        p.consorcio,
        p.vista,
        p.tipo_dia,
        v.*,
        time("2022-06-01 00:00:00") as inicio_periodo,
        time("2022-06-01 23:59:59") as fim_periodo,
        0 as tempo_planejado
    from viagem_planejada p
    inner join recursos_viagem v
    on 
        v.trip_id = p.trip_id
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
select distinct
    protocolo,
    consorcio,
    data,
    tipo_dia,
    id_empresa,
    id_veiculo,
    id_viagem,
    servico_informado,
    servico_realizado,
    vista,
    trip_id,
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
    perc_conformidade_distancia > {{ var("perc_conformidade_distancia_recurso_min") }}
)
and (
    perc_conformidade_registros >= {{ var("perc_conformidade_registros_min") }}
)