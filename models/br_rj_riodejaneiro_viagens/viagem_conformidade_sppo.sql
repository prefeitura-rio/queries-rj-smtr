{{ config(
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

-- 1. Agrega informações de viagens circulares: ajusta
--    datetime_chegada, calcula tempo total de
--    viagem e distancia total planejada. Mantem o shape planejado ("C")
--    como padrão da viagem.
with viagem as (
    select
        *,
        datetime_diff(datetime_chegada, datetime_partida, minute) + 1 as tempo_viagem
    from (
        select 
            *
        from (
            select
                * except(sentido_shape, distancia_inicio_fim, shape_id, shape_id_planejado, trip_id, trip_id_planejado, datetime_chegada),
                datetime_chegada,
                trip_id_planejado as trip_id,
                shape_id_planejado as shape_id
            from 
                {{ ref("aux_viagem_circular") }} v
            where 
                sentido = "I" or sentido = "V"
        )
        union all (
            select 
                * except(sentido_shape, distancia_inicio_fim, shape_id, shape_id_planejado, trip_id, trip_id_planejado),
                trip_id_planejado as trip_id,
                shape_id_planejado as shape_id,
            from 
                (select 
                    v.* except(datetime_chegada),
                    lead(datetime_chegada) over (
                        partition by id_viagem order by sentido_shape)
                    as datetime_chegada,
                    -- TODO: mudar se tiver distancia planejada separada
                    -- por shape (ida/volta)
                    -- distancia_planejada,
                    -- round(distancia_planejada + lead(distancia_planejada) over (
                    --     partition by id_viagem order by sentido_shape), 3)
                    -- as distancia_planejada,
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
    round(100 * n_registros_shape/n_registros_total, 2) as perc_conformidade_shape,
    round(100 * d.distancia_aferida/v.distancia_planejada, 2) as perc_conformidade_distancia,
    round(100 * n_registros_minuto/tempo_viagem, 2) as perc_conformidade_registros,
    '{{ var("version") }}' as versao_modelo
from 
    viagem v
inner join 
    {{ ref("aux_viagem_registros") }} d
on
    v.id_viagem = d.id_viagem