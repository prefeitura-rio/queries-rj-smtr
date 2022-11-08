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


WITH registros_viagem AS (
    SELECT
        s.*, -- EXCEPT(versao_modelo),
        v.datetime_partida,
        v.datetime_chegada,
        v.distancia_inicio_fim,
        v.id_viagem
    FROM
        `rj-smtr-dev.viagens.aux_registros_status_trajeto` AS s --{{ ref("aux_registros_status_trajeto") }} s
    LEFT JOIN (
        SELECT
            id_veiculo,
            --trip_id,
            servico_realizado,
            sentido_shape,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            distancia_inicio_fim
        FROM
            `rj-smtr-dev.viagens.aux_viagem_circular` --{{ ref("aux_viagem_circular") }}
    ) AS v
    ON
        s.id_veiculo = v.id_veiculo
        AND s.servico_realizado = v.servico_realizado
        AND s.timestamp_gps BETWEEN v.datetime_partida AND v.datetime_chegada
)
-- 2. Filtra apenas registros de viagens identificadas
SELECT
    *,
    --'{{ var("version") }}' as versao_modelo
FROM
    registros_viagem
WHERE
    id_viagem IS NOT NULL