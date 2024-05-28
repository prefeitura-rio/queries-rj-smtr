{{
  config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key="id_gratuidade",
    partition_by={
      "field": "id_cliente",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 100000000,
        "interval": 10000
      }
    },
  )
}}


{% set staging_gratuidade = ref('staging_gratuidade') %}

{% set incremental_filter %}
    DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
    AND timestamp_captura BETWEEN DATETIME("{{var('date_range_start')}}") AND DATETIME("{{var('date_range_end')}}")
{% endset %}

{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            SELECT DISTINCT
                CAST(CAST(cd_cliente AS FLOAT64) AS INT64) AS cd_cliente
            FROM
                {{ staging_gratuidade }}
            WHERE
                {{ incremental_filter }}
        {% endset %}

        {% set partitions = run_query(partitions_query) %}

        {% set partition_list = partitions.columns[0].values() %}
    {% endif %}
{% endif %}

WITH gratuidade_complete_partitions AS (
    SELECT
        CAST(CAST(cd_cliente AS FLOAT64) AS INT64) AS id_cliente,
        id AS id_gratuidade,
        tipo_gratuidade,
        data_inclusao AS data_inicio_validade,
        timestamp_captura
    FROM
        {{ staging_gratuidade }}
    {% if is_incremental() -%}
        WHERE
        {{ incremental_filter }}
    
        {% if partition_list|length > 0 -%}
            UNION ALL

            SELECT
                id_cliente,
                id_gratuidade,
                tipo_gratuidade,
                data_inicio_validade,
                timestamp_captura
            FROM
                {{ this }}
            WHERE
                id_cliente IN ({{ partition_list|join(', ') }})
        {%- endif %}
    {%- endif %}
),
gratuidade_deduplicada AS (
    SELECT
        * EXCEPT(rn)
    FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_gratuidade ORDER BY timestamp_captura DESC) AS rn
            FROM
                gratuidade_complete_partitions
        )
    WHERE
        rn = 1
)
SELECT
    id_cliente,
    id_gratuidade,
    tipo_gratuidade,
    data_inicio_validade,
    LEAD(data_inicio_validade) OVER (PARTITION BY id_cliente ORDER BY data_inicio_validade) AS data_fim_validade,
    timestamp_captura
FROM
    gratuidade_deduplicada