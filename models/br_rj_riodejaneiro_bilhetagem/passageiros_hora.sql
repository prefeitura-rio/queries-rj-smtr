{{
  config(
        materialized='incremental',
        partition_by={
            "field":"data",
            "data_type": "date",
            "granularity":"day"
        },
        incremental_strategy="insert_overwrite"
    )
}}

{% set transacao_staging = ref('staging_transacao') %}
{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            SELECT DISTINCT
                CONCAT("'", DATE(data_transacao), "'") AS data_transacao
            FROM
                {{ transacao_staging }}
            WHERE
                DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
        {% endset %}

        {% set partitions = run_query(partitions_query) %}

        {% set partition_list = partitions.columns[0].values() %}
    {% endif %}
{% endif %}

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
        t.tipo_gratuidade,
        t.tipo_pagamento,
        COUNT(t.id_transacao) AS quantidade_passageiros
    FROM
        {{ ref("transacao") }} t
    LEFT JOIN
        {{ ref("integracao") }} i
    ON
        t.id_transacao = i.id_transacao
    WHERE
        {% if is_incremental() %}
            {% if partition_list|length > 0 %}
                t.data IN ({{ partition_list|join(', ') }})
            {% else %}
                t.data = "2000-01-01"
            {% endif %}
        {% else %}
            t.data >= "2023-07-19"
        {% endif %}
        AND t.servico NOT IN ("888888", "999999")
        AND t.id_operadora != "2"
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9
),
transacao_tratada AS (
    SELECT
        t.data,
        t.hora,
        t.modo,
        t.consorcio,
        t.servico,
        t.sentido,
        CASE
            WHEN t.tipo_transacao = "Integração" THEN "Integração"
            WHEN t.tipo_transacao IN ("Débito", "Botoeira") THEN "Tarifa Cheia"
            ELSE t.tipo_transacao
        END AS tipo_transacao_smtr,
        t.tipo_gratuidade,
        t.tipo_pagamento,
        t.quantidade_passageiros
    FROM
        transacao_agrupada t
)
SELECT
    t.data,
    t.hora,
    t.modo,
    t.consorcio,
    t.servico,
    t.sentido,
    t.tipo_transacao_smtr,
    CASE
        WHEN t.tipo_transacao_smtr = "Gratuidade" THEN t.tipo_gratuidade
        WHEN t.tipo_transacao_smtr = "Integração" THEN "Integração"
        ELSE t.tipo_pagamento
    END AS tipo_transacao_detalhe_smtr,
    t.quantidade_passageiros
FROM
    transacao_tratada t
