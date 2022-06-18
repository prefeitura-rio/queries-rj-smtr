{{ config(
       materialized='incremental',
       partition_by={
              "field":"data",
              "data_type": "date",
              "granularity":"day"
       }
)
}}

-- 1. Identifica registros pertencentes a viagens
with registros_viagem as (
    select
    s.* except(versao_modelo),
    datetime_partida,
    datetime_chegada,
    distancia_inicio_fim,
    id_viagem
    from 
        {{ ref("aux_registros_status_trajeto") }} s
    left join (
    select 
        id_veiculo,
        servico_realizado,
        sentido_shape,
        id_viagem,
        datetime_partida,
        datetime_chegada,
        distancia_inicio_fim
    from
        {{ ref("aux_viagem_circular") }}
    ) v
    on 
    s.id_veiculo = v.id_veiculo
    and s.servico_realizado = v.servico_realizado
    and s.sentido_shape = v.sentido_shape
    and s.timestamp_gps between v.datetime_partida and v.datetime_chegada
)
-- 2. Filtra apenas registros de viagens identificadas
select 
    *,
    '{{ var("version") }}' as versao_modelo
from 
    registros_viagem
where
    id_viagem is not null