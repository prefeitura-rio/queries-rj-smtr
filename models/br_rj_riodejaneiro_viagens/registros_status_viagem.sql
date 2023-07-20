{{ config(
       materialized='incremental',
       partition_by={
              "field":"data",
              "data_type": "date",
              "granularity":"day"
       },
       unique_key=['timestamp_gps', 'id_veiculo'],
       incremental_strategy='insert_overwrite'
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
        trip_id,
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
    and s.trip_id = v.trip_id
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