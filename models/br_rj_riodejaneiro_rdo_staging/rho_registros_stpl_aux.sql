{{ 
  config(
    materialized='incremental',
    incremental_strategy="insert_overwrite",
    partition_by={
        "field":"data_transacao",
        "data_type": "date",
        "granularity":"day"
    },
  )
}}

WITH rho_new AS (
    SELECT
        data_transacao,
        hora_transacao,
        data_particao AS data_arquivo_rho,
        linha AS servico_riocard,
        operadora,
        total_pagantes AS quantidade_transacao_pagante,
        total_gratuidades AS quantidade_transacao_gratuidade,
        timestamp_captura AS datetime_captura
    FROM
        {{ ref('staging_rho_registros_stpl') }}
    {% if is_incremental() %}
        WHERE
            ano BETWEEN 
                EXTRACT(YEAR FROM DATE("{{ var('date_range_start') }}")) 
                AND EXTRACT(YEAR FROM DATE("{{ var('date_range_end') }}"))
            AND mes BETWEEN 
                EXTRACT(MONTH FROM DATE("{{ var('date_range_start') }}")) 
                AND EXTRACT(MONTH FROM DATE("{{ var('date_range_end') }}"))
            AND dia BETWEEN 
                EXTRACT(DAY FROM DATE("{{ var('date_range_start') }}")) 
                AND EXTRACT(DAY FROM DATE("{{ var('date_range_end') }}"))
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
SELECT
    data_transacao,
    hora_transacao,
    data_arquivo_rho,
    servico_riocard,
    operadora,
    quantidade_transacao_pagante,
    quantidade_transacao_gratuidade,
    datetime_captura
FROM
    (
        SELECT 
            *,
            ROW_NUMBER() OVER(
                PARTITION BY 
                    data_transacao,
                    hora_transacao,
                    data_arquivo_rho,
                    servico_riocard,
                    operadora
                ORDER BY
                    datetime_captura DESC
                ) AS rn
        FROM
            rho_complete_partitions
    )
WHERE
    rn = 1