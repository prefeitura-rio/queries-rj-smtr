{{ config(
       materialized='incremental',
       partition_by={
              "field":"data",
              "data_type": "date",
              "granularity":"day"
       }
)
}}

-- 1. Identifica e junta viagens de ida seguidas de volta circulares
with viagem_circular as (
    select 
        * except(
            datetime_chegada, 
            datetime_chegada_volta
        ),
        datetime_chegada_volta as datetime_chegada
    from (
        select 
            *,
            lead(datetime_partida) over (
                partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) as datetime_partida_volta,
            lead(datetime_chegada) over (
                partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) as datetime_chegada_volta,
            lead(sentido) over (
                partition by id_veiculo, servico_realizado order by id_veiculo, servico_realizado, datetime_partida, sentido) = "V" as flag_proximo_volta -- possui volta
        from 
            {{ ref("aux_viagem_inicio_fim") }} v
        where
            sentido = "C"
    ) t
    where
        flag_proximo_volta = TRUE
        and sentido = "I"
),
-- 2. Junta viagens circulares a não circulares, e calcula tempo de viagem
viagem as (
    select 
        *,
        datetime_diff(datetime_chegada, datetime_partida, minute) + 1 as tempo_viagem
    from (
        select 
            *
        from (
            select 
                data,
                tipo_dia,
                consorcio,
                id_veiculo,
                id_empresa,
                servico_informado,
                servico_realizado,
                shape_id,
                sentido,
                id_viagem,
                datetime_partida,
                datetime_chegada
            from 
                viagem_circular
        ) 
        union all (
            select 
                data,
                tipo_dia,
                consorcio,
                id_veiculo,
                id_empresa,
                servico_informado,
                servico_realizado,
                shape_id,
                sentido,
                id_viagem,
                datetime_partida,
                datetime_chegada
            from 
                {{ ref("aux_viagem_inicio_fim") }} v
            where
                sentido != "C"
        )
    ) t
)
select 
    *,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    viagem