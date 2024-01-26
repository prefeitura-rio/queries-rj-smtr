{{ 
  config(
      materialized='table',
      partition_by={
            "field":"data_transacao",
            "data_type": "date",
            "granularity":"day"
      },
  )
}}

WITH rho_stpl AS (
    SELECT
        data_transacao,
        hora_transacao,
        data_particao AS data_arquivo_rho,
        linha,
        operadora,
        total_pagantes,
        total_gratuidades,
        timestamp_captura
    FROM
        {{ ref('staging_rho_registros_stpl') }}
),
rho_rn AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY 
                data_transacao, 
                hora_transacao,
                data_arquivo_rho,
                linha,
                operadora 
            ORDER BY 
                timestamp_captura DESC
        ) AS rn
    FROM
        rho_stpl
)
SELECT
    data_transacao,
    hora_transacao,
    linha AS servico_rio_card,
    operadora,
    SUM(total_pagantes) AS  quantidade_transacao_pagante,
    SUM(total_gratuidades) AS quantidade_transacao_gratuidade
FROM
    rho_rn
WHERE
    rn = 1
GROUP BY
    1,
    2,
    3,
    4

