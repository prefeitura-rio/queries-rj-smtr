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


WITH partitions AS (
    SELECT DISTINCT
        data_transacao
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
)
SELECT
    data_transacao,
    hora_transacao,
    servico_riocard,
    operadora,
    SUM(quantidade_transacao_pagante) AS quantidade_transacao_pagante,
    SUM(quantidade_transacao_gratuidade) AS quantidade_transacao_gratuidade
FROM
    {{ ref('rho_registros_stpl_aux') }}
WHERE
    data_transacao IN (SELECT data_transacao FROM partitions)
GROUP BY
    1,
    2,
    3,
    4