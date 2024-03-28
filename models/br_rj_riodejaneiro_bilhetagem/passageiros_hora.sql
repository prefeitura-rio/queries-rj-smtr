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


/* 
consulta as partições a serem atualizadas com base nas transações capturadas entre date_range_start e date_range_end
e as integrações capturadas entre date_range_start e date_range_end
*/
{% set transacao_staging = ref('staging_transacao') %}
{% set integracao_staging = ref('staging_integracao_transacao') %}
{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            WITH particoes_integracao AS (
                SELECT DISTINCT
                    CONCAT("'", DATE(data_transacao), "'") AS data_transacao
                FROM
                    {{ integracao_staging }},
                    UNNEST([
                        data_transacao_t0,
                        data_transacao_t1,
                        data_transacao_t2,
                        data_transacao_t3,
                        data_transacao_t4
                    ]) AS data_transacao
                WHERE
                    DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")

            ),
            particoes_transacao AS (
                SELECT DISTINCT
                    CONCAT("'", DATE(data_transacao), "'") AS data_transacao
                FROM
                    {{ transacao_staging }}
                WHERE
                    DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
            )
            SELECT
                COALESCE(t.data_transacao, i.data_transacao) AS data_transacao
            FROM
                particoes_transacao t
            FULL OUTER JOIN
                particoes_integracao i
            USING(data_transacao)
            WHERE COALESCE(t.data_transacao, i.data_transacao) IS NOT NULL
                
        {% endset %}

        {% set partitions = run_query(partitions_query) %}

        {% set partition_list = partitions.columns[0].values() %}
    {% endif %}
{% endif %}

WITH integracao AS (
    SELECT
        *
    FROM
        {{ ref("integracao") }}
    {% if is_incremental() %}
        WHERE
            {% if partition_list|length > 0 %}
                data IN ({{ partition_list|join(', ') }})
            {% else %}
                data = "2000-01-01"
            {% endif %}
    {% endif %}
),
transacao_agrupada AS (
    SELECT
        t.data,
        t.hora,
        t.modo,
        t.consorcio,
        t.id_servico_jae,
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
        integracao i
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
        AND t.id_servico_jae NOT IN ("140", "142")
        AND t.id_operadora != "2"
        AND (t.modo = "BRT" OR (t.modo = "VLT" AND t.data >= DATE("2024-02-24")))
        AND t.tipo_transacao IS NOT NULL
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
        t.id_servico_jae,
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
    t.id_servico_jae,
    t.sentido,
    t.tipo_transacao_smtr,
    CASE
        WHEN t.tipo_transacao_smtr = "Gratuidade" THEN t.tipo_gratuidade
        WHEN t.tipo_transacao_smtr = "Integração" THEN "Integração"
        WHEN t.tipo_transacao_smtr = "Transferência" THEN "Transferência"
        ELSE t.tipo_pagamento
    END AS tipo_transacao_detalhe_smtr,
    t.quantidade_passageiros,
    '{{ var("version") }}' as versao
FROM
    transacao_tratada t
