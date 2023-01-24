{{ 
config(
    materialized='incremental',
    partition_by={
            "field":"data",
            "data_type": "date",
            "granularity":"day"
    },
    unique_key=['id_viagem'],
    incremental_strategy='insert_overwrite'
)
}}
-- 1. Identifica viagens que estão dentro do quadro planejado (por
--    enquanto, consideramos o dia todo).
with viagem_periodo as (
    select distinct
        p.consorcio,
        p.vista,
        p.tipo_dia,
        v.*,
        p.inicio_periodo,
        p.fim_periodo,
        0 as tempo_planejado
    from (
        select distinct
            consorcio,
            vista,
            data,
            tipo_dia,
            trip_id_planejado as trip_id,
            servico,
            inicio_periodo,
            fim_periodo
        from
            {{ ref("viagem_planejada") }}
        {% if is_incremental() %}
        WHERE
            data = date_sub(date("{{ var("run_date") }}"), interval 1 day)
        {% endif %}
    ) p
    inner join (
        select distinct * from {{ ref("viagem_conformidade") }} 
        {% if is_incremental() %}
        WHERE 
            data = date_sub(date("{{ var("run_date") }}"), interval 1 day)
        {% endif %}
    ) v 
    on 
        v.trip_id = p.trip_id
        and v.data = p.data
        -- Reveillon (2022-12-31)
        {% if var("run_date") == "2023-01-01" %}
        where
            (
                -- 1. Viagens pre fechamento das vias
                (p.fim_periodo = "22:00:00" and datetime_chegada <= "2023-12-31 22:05:00")
                or 
                (p.fim_periodo = "18:00:00" and datetime_chegada <= "2023-12-31 18:05:00") -- 18h as 5h
                or 
                -- 2. Viagens durante fechamento das vias
                (p.inicio_periodo = "22:00:00" and datetime_partida >= "2023-12-31 21:55:00") -- 22h as 5h/10h
                or
                (p.inicio_periodo = "18:00:00" and datetime_partida >= "2023-12-31 17:55:00") -- 18h as 5h
                or
                -- 3. Viagens que nao sao afetadas pelo fechamento das vias
                (p.inicio_periodo = "00:00:00" and p.fim_periodo = "23:59:59")
            )
        -- Feriado do Dia da Fraternidade Universal (2023-01-01)
        {% elif var("run_date") == "2023-01-02" %}
        where
            (
                -- 1. Viagens durante fechamento das vias
                (p.fim_periodo = "05:00:00" and datetime_partida <= "2023-01-01 05:05:00")
                or 
                (p.fim_periodo = "10:00:00" and datetime_partida <= "2023-01-01 10:05:00")
                or 
                -- 2. Viagens pos abertura das vias
                (p.inicio_periodo = "05:00:00" and datetime_partida >= "2023-01-01 04:55:00")
                or
                (p.inicio_periodo = "10:00:00" and datetime_partida >= "2023-01-01 09:55:00")
                or 
                -- 3. Viagens que nao sao afetadas pelo fechamento das vias
                (p.inicio_periodo = "00:00:00" and p.fim_periodo = "23:59:59")
            )
        {% endif %}
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
    perc_conformidade_distancia >= {{ var("perc_conformidade_distancia_min") }}
)
and (
    perc_conformidade_registros >= {{ var("perc_conformidade_registros_min") }}
)
