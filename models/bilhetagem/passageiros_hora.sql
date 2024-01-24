{{
  config(
    materialized="view",
  )
}}

WITH transacao_agrupada AS (
    SELECT
        t.data,
        t.hora,
        t.modo,
        t.consorcio,
        t.servico,
        t.sentido,
        CASE
            WHEN i.id_integracao IS NOT NULL THEN "Integração"
            ELSE t.tipo_transacao
        END AS tipo_transacao,
        COUNT(t.id_transacao) AS quantidade_passageiros,
        -- ROUND(100 * COUNT(t.id_transacao) / SUM(COUNT(*)) OVER (PARTITION BY t.data, t.modo), 2) AS percentual_passageiros_modo,
        ROUND(100 * COUNT(t.id_transacao) / SUM(COUNT(*)) OVER (PARTITION BY t.data), 2) AS percentual_passageiros_dia
    FROM
        {{ ref("transacao") }} t
    LEFT JOIN
        {{ ref("integracao") }} i
    ON
        t.id_transacao = i.id_transacao
    WHERE
        t.data >= "2023-07-19"
        AND t.servico NOT IN ("888888",
        "999999")
        AND t.id_operadora != "2"
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