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
        timestamp_captura AS datetime_captura
    FROM
        {{ ref('rho_registros_stpl_view') }}
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
                datetime_captura DESC
        ) AS rn
    FROM
        rho_stpl
)
SELECT
    * EXCEPT(rn)
FROM
    rho_rn
WHERE
    rn = 1

