{{ 
  config(
      materialized='incremental',
      partition_by={
            "field":"data_transacao",
            "data_type": "date",
            "granularity":"day"
      },
      incremental_strategy="insert_overwrite"
  )
}}

WITH rho_new AS (
    SELECT
        data_transacao,
        hora_transacao,
        data_processamento,
        data_particao AS data_arquivo_rho,
        linha,
        linha_rcti,
        operadora,
        total_pagantes_cartao,
        total_pagantes_especie,
        total_gratuidades,
        registro_processado,
        timestamp_captura AS datetime_captura
    FROM
        {{ ref('rho_registros_sppo_view') }}
    {% if is_incremental() %}
        WHERE
            timestamp_captura > DATETIME("{{ var('date_range_start') }}")
            AND timestamp_captura <= DATETIME("{{ var('date_range_end') }}") 
    {% endif %}
),
rho_complete_partitions AS (
    SELECT
        *
    FROM
        rho_new
    
    {% if is_incremental() %}
    
        UNION ALL

        SELECT
            *
        FROM
            {{ this }}
        WHERE
            data_transacao IN (SELECT DISTINCT data_transacao FROM rho_new)
    
    {% endif %}
)
rho_rn AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY 
                data_transacao, 
                hora_transacao,
                data_arquivo_rho,
                linha_rcti,
                operadora 
            ORDER BY 
                datetime_captura DESC
        ) AS rn
    FROM
        rho_complete_partitions
)
SELECT
    * EXCEPT(rn)
FROM
    rho_rn
WHERE
    rn = 1