{{
  config(
    materialized="view",
  )
}}

WITH transacao_agrupada AS (
    SELECT
        data,
        hora,
        modo,
        consorcio,
        servico,
        sentido,
        CASE
            WHEN indicador_integracao = TRUE THEN "Integração"
            ELSE tipo_transacao
        END AS tipo_transacao,
        COUNT(id_transacao) AS quantidade_passageiros,
        -- ROUND(100 * COUNT(id_transacao) / SUM(COUNT(*)) OVER (PARTITION BY DATA, modo), 2) AS percentual_passageiros_modo,
        ROUND(100 * COUNT(id_transacao) / SUM(COUNT(*)) OVER (PARTITION BY DATA), 2) AS percentual_passageiros_dia
    FROM
        {{ ref("transacao") }}
    WHERE
        data >= "2023-07-19"
        AND servico NOT IN ("888888",
        "999999")
        AND id_operadora != "2"
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7
)
SELECT
    t.data,
    t.hora,
    t.modo,
    t.consorcio,
    t.servico,
    t.sentido,
    t.tipo_transacao,
    t.quantidade_passageiros
FROM
    transacao_agrupada t
ORDER BY
    percentual_passageiros_dia DESC