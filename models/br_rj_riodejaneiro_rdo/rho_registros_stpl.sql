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

{% set staging_table = ref('staging_rho_registros_stpl') %}
{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            SELECT DISTINCT
                CONCAT("'", data_transacao, "'")
            FROM
                {{ staging_table }}
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
        {% endset %}

        {% set partitions = run_query(partitions_query) %}

        {% set partition_list = partitions.columns[0].values() %}
    {% endif %}
{% endif %}

SELECT
    data_transacao,
    hora_transacao,
    servico_riocard,
    operadora,
    SUM(quantidade_transacao_pagante) AS quantidade_transacao_pagante,
    SUM(quantidade_transacao_gratuidade) AS quantidade_transacao_gratuidade
FROM
    {{ ref('rho_registros_stpl_aux') }}
{% if is_incremental() %}
    WHERE
        data_transacao
        {% if partition_list|length > 0 %}
            IN ({{ partition_list|join(', ') }})
        {% else %}
            = "2000-01-01"
        {% endif %}
{% endif %}
GROUP BY
    1,
    2,
    3,
    4