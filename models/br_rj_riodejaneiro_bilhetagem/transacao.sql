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

WITH transacao_deduplicada AS (
    SELECT 
        * EXCEPT(rn)
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
        FROM
            {{ ref("staging_transacao") }}
        {% if is_incremental() -%}
        WHERE
            DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
            AND timestamp_captura BETWEEN DATETIME("{{var('date_range_start')}}") AND DATETIME("{{var('date_range_end')}}")
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
    l.nr_linha AS servico,
    sentido,
    NULL AS id_veiculo,
    COALESCE(id_cliente, pan_hash) AS id_cliente,
    id AS id_transacao,
    id_tipo_midia AS id_tipo_pagamento,
    tt.tipo_transacao,
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
    {{ ref("staging_linha") }} AS l
ON
    t.cd_linha = l.cd_linha 
    AND t.data_transacao >= l.datetime_inclusao
LEFT JOIN 
    {{ source("cadastro", "modos") }} m
ON
  t.id_tipo_modal = m.id_modo AND sistema = "jae"
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