-- 1. Identifica viagens circulares de ida que possuem volta
--    consecutiva. Junta numa única linha a datetime_partida (ida) + datetime_chegada_volta
WITH ida_volta_circular AS (
    SELECT 
        t.*
    FROM (
        SELECT 
            *,
            LEAD(datetime_partida) OVER (
                PARTITION BY id_veiculo, servico_realizado ORDER BY id_veiculo, servico_realizado, datetime_partida, sentido_shape) AS datetime_partida_volta,
            LEAD(datetime_chegada) OVER (
                PARTITION BY id_veiculo, servico_realizado ORDER BY id_veiculo, servico_realizado, datetime_partida, sentido_shape) AS datetime_chegada_volta,
            LEAD(shape_id) OVER (
                PARTITION BY id_veiculo, servico_realizado ORDER BY id_veiculo, servico_realizado, datetime_partida, sentido_shape) AS shape_id_volta,
            LEAD(sentido_shape) OVER (
                PARTITION BY id_veiculo, servico_realizado ORDER BY id_veiculo, servico_realizado, datetime_partida, sentido_shape) = "V" AS flag_proximo_volta -- possui volta
        FROM 
            {{ ref('aux_viagem_inicio_fim') }} AS v
        WHERE
            sentido = "C"
    ) AS t
    WHERE
        flag_proximo_volta = TRUE
        AND sentido_shape = "I"
        AND datetime_chegada <= datetime_partida_volta
),
-- 2. Filtra apenas viagens circulares de ida e volta consecutivas
--    (mantem ida e volta separadas, mas com o mesmo id)
viagem_circular AS (
    SELECT DISTINCT
        * 
    FROM (
        SELECT
            CASE
                WHEN (
                    v.sentido_shape = "I"
                    AND v.datetime_partida = c.datetime_partida
                ) THEN c.id_viagem
                WHEN (
                    v.sentido_shape = "V"
                    AND v.datetime_chegada = c.datetime_chegada_volta
                ) THEN c.id_viagem
            END AS id_viagem,
            v.* EXCEPT(id_viagem)
        FROM 
            {{ ref('aux_viagem_inicio_fim') }} AS v
        INNER JOIN
            ida_volta_circular AS c
        ON
            c.id_veiculo = v.id_veiculo
            AND c.servico_realizado = v.servico_realizado
            AND c.sentido = v.sentido
    ) AS v
    WHERE
        id_viagem IS NOT NULL
)
-- 3. Junta viagens circulares tratadas às viagens não circulares já identificadas
SELECT
    *
FROM 
    viagem_circular v
UNION ALL (
    SELECT
        *
    FROM
        {{ ref('aux_viagem_inicio_fim') }}
    WHERE 
        (sentido = "I" OR sentido = "V")
)