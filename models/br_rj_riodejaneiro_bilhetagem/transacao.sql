-- depends_on: {{ ref('operadoras_contato') }}
{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
    unique_key="id_transacao",
  )
}}

-- Verifica partições para consultar na tabela de gratuidades
{% set incremental_filter %}
    DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
    AND timestamp_captura BETWEEN DATETIME("{{var('date_range_start')}}") AND DATETIME("{{var('date_range_end')}}")
{% endset %}

{% set transacao_staging = ref('staging_transacao') %}
{% if execute %}
    {% if is_incremental() %}
        {% set gratuidade_partitions_query %}
            SELECT DISTINCT
                CAST(CAST(id_cliente AS FLOAT64) AS INT64) AS id_cliente
            FROM
                {{ transacao_staging }}
            WHERE
                {{ incremental_filter }}
                AND tipo_transacao = "21"
        {% endset %}

        {% set gratuidade_partitions = run_query(gratuidade_partitions_query) %}

        {% set gratuidade_partition_list = gratuidade_partitions.columns[0].values() %}
    {% endif %}
{% endif %}

WITH transacao_deduplicada AS (
    SELECT 
        * EXCEPT(rn)
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
        FROM
            {{ transacao_staging }}
        {% if is_incremental() -%}
            WHERE
                {{ incremental_filter }}
        {%- endif %}
    )
    WHERE
        rn = 1
),
tipo_transacao AS (
  SELECT
    chave AS id_tipo_transacao,
    valor AS tipo_transacao,
  FROM
    `rj-smtr.br_rj_riodejaneiro_bilhetagem.dicionario`
  WHERE
    id_tabela = "transacao"
    AND coluna = "id_tipo_transacao" 
),
gratuidade AS (
    SELECT 
        CAST(id_cliente AS STRING) AS id_cliente,
        tipo_gratuidade,
        data_inicio_validade,
        data_fim_validade
    FROM
        {{ ref("gratuidade_aux") }}
    -- se for incremental pega apenas as partições necessárias
    {% if is_incremental() %}
        {% if gratuidade_partition_list|length > 0 and gratuidade_partition_list|length < 10000 %}
            WHERE
                id_cliente IN ({{ gratuidade_partition_list|join(', ') }})
        {% elif gratuidade_partition_list|length == 0 %}
            WHERE
                id_cliente = 0
        {% endif %}
    {% endif %}
),
tipo_pagamento AS (
  SELECT
    chave AS id_tipo_pagamento,
    valor AS tipo_pagamento
  FROM
    `rj-smtr.br_rj_riodejaneiro_bilhetagem.dicionario`
  WHERE
    id_tabela = "transacao"
    AND coluna = "id_tipo_pagamento" 
)
SELECT 
    EXTRACT(DATE FROM data_transacao) AS data,
    EXTRACT(HOUR FROM data_transacao) AS hora,
    data_transacao AS datetime_transacao,
    data_processamento AS datetime_processamento,
    t.timestamp_captura AS datetime_captura,
    m.modo,
    dc.id_consorcio,
    dc.consorcio,
    do.id_operadora,
    do.operadora,
    t.cd_linha AS id_servico_jae,
    sentido,
    NULL AS id_veiculo,
    COALESCE(t.id_cliente, t.pan_hash) AS id_cliente,
    id AS id_transacao,
    tp.tipo_pagamento,
    tt.tipo_transacao,
    g.tipo_gratuidade,
    tipo_integracao AS id_tipo_integracao,
    NULL AS id_integracao,
    latitude_trx AS latitude,
    longitude_trx AS longitude,
    NULL AS stop_id,
    NULL AS stop_lat,
    NULL AS stop_lon,
    valor_transacao,
    '{{ var("version") }}' as versao
FROM
    transacao_deduplicada AS t
LEFT JOIN 
    {{ source("cadastro", "modos") }} m
ON
    t.id_tipo_modal = m.id_modo AND m.fonte = "jae"
LEFT JOIN
    {{ ref("operadoras") }} AS do
ON
    t.cd_operadora = do.id_operadora_jae
LEFT JOIN
    {{ ref("consorcios") }} AS dc
ON
    t.cd_consorcio = dc.id_consorcio_jae
LEFT JOIN
    tipo_transacao AS tt
ON
    tt.id_tipo_transacao = t.tipo_transacao
LEFT JOIN
    tipo_pagamento tp
ON
    t.id_tipo_midia = tp.id_tipo_pagamento
LEFT JOIN
    gratuidade g
ON
    t.tipo_transacao = "21"
    AND t.id_cliente = g.id_cliente
    AND t.data_transacao >= g.data_inicio_validade
    AND (t.data_transacao < g.data_fim_validade OR g.data_fim_validade IS NULL)
LEFT JOIN
    {{ ref("staging_linha_sem_ressarcimento") }} l
ON
    t.cd_linha = l.id_linha
WHERE
    l.id_linha IS NULL
    AND DATE(data_transacao) >= "2023-07-17"