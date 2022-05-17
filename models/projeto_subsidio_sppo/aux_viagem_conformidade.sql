{{ config(
       materialized='incremental',
       partition_by={
              "field":"data",
              "data_type": "date",
              "granularity":"day"
       }
)
}}

-- 1. Ajusta datetime_chegada da viagem circular e calcula tempo de viagem
with viagem as (
    select
        *,
        datetime_diff(datetime_chegada, datetime_partida, minute) + 1 as tempo_viagem
    from (
        select 
            *
        from (
            select
                * except(sentido_shape, datetime_chegada),
                datetime_chegada,
            from 
                {{ ref("aux_viagem_circular") }} v
            where 
                sentido = "I" or sentido = "V"
        )
        union all (
            select 
                * except(
                    sentido_shape, 
                    datetime_chegada),
                datetime_chegada,
            from 
                (select 
                    v.* except(datetime_chegada),
                    lead(datetime_chegada) over (
                        partition by id_veiculo, servico_realizado 
                        order by id_veiculo, servico_realizado, datetime_partida, sentido_shape) 
                    as datetime_chegada,
                from 
                    {{ ref("aux_viagem_circular") }} v
                where 
                    sentido = "C"
                ) c
            where sentido_shape = "I"
        )
    )
)
-- 2. Calcula os percentuais de conformidade da distancia, trajeto e GPS
select distinct
    v.* except(versao_modelo),
    d.* except(id_viagem, versao_modelo),
    round(100 * n_registros_shape/n_registros_total,2) as perc_conformidade_shape,
    round(100 * d.distancia_aferida/v.distancia_planejada, 2) as perc_conformidade_distancia,
    round(100 * n_registros_minuto/tempo_viagem, 2) as perc_conformidade_registros,
    '{{ var("projeto_subsidio_sppo_version") }}' as versao_modelo
from 
    viagem v -- {{ ref("aux_viagem_circular") }} v
inner join 
    {{ ref("aux_viagem_distancia") }} d
on
    v.id_viagem = d.id_viagem
    -- and v.data = d.data