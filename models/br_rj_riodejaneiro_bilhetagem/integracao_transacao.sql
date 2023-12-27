-- depends_on: {{ ref('matriz_integracao') }}
{{
  config(
    alias="integracao",
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
    unique_key='id_transacao'
  )
}}

WITH integracao_transacao_deduplicada AS (
  SELECT 
    * EXCEPT(rn)
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
    FROM
      {{ ref("staging_integracao_transacao") }}
    {% if is_incremental() -%}
      WHERE
        DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
        AND timestamp_captura BETWEEN DATETIME("{{var('date_range_start')}}") AND DATETIME("{{var('date_range_end')}}")
    {%- endif %}
  )
  WHERE
    rn = 1
),
integracao_melt AS (
    SELECT
      EXTRACT(DATE FROM i.data_processamento) AS data,
      EXTRACT(HOUR FROM i.data_processamento) AS hora,
      i.data_inclusao AS datetime_inclusao,
      i.data_processamento AS datetime_processamento,
      i.timestamp_captura AS datetime_captura,
      i.id AS id_integracao,
      im.sequencia_integracao,
      im.data_transacao,
      im.id_tipo_modal,
      im.id_consorcio,
      im.id_operadora,
      im.id_linha,
      im.id_transacao,
      im.sentido,
      im.perc_rateio,
      im.valor_rateio_compensacao,
      im.valor_rateio,
      im.valor_transacao,
      i.valor_transacao_total,
      i.tx_adicional AS texto_adicional
    FROM
      integracao_transacao_deduplicada i,
      -- Transforma colunas com os dados de cada transação da integração em linhas diferentes
      UNNEST(
        [
          {% for n in range(var('quantidade_integracoes_max')) %}
            STRUCT(
              {% for column, column_config in var('colunas_integracao').items() %}
                {% if  column_config.select %}
                  {{ column }}_t{{ n }} AS {{ column }},
                {% endif %}
              {% endfor %}
              {{ n }} AS sequencia_integracao
            ){% if not loop.last %},{% endif %}
          {% endfor %}
        ]
      ) AS im
)
SELECT 
  i.data,
  i.hora,
  i.datetime_processamento,
  i.datetime_captura,
  i.id_integracao,
  i.sequencia_integracao,
  m.modo,
  dc.id_consorcio,
  dc.consorcio,
  do.id_operadora,
  do.operadora,
  l.nr_linha AS servico,
  i.id_transacao,
  i.sentido,
  i.perc_rateio AS percentual_rateio,
  i.valor_rateio_compensacao,
  i.valor_rateio,
  i.valor_transacao,
  i.valor_transacao_total,
  i.texto_adicional,
  '{{ var("version") }}' as versao
FROM
  integracao_melt i
LEFT JOIN
  {{ ref("staging_linha") }} AS l
ON
  i.id_linha = l.cd_linha
  AND i.data_transacao >= l.datetime_inclusao
LEFT JOIN
    {{ ref("diretorio_modos") }} AS m
ON 
    (i.id_tipo_modal = m.id_modo_jae AND (m.id_consorcio_jae IS NULL OR i.id_consorcio = m.id_consorcio_jae))
LEFT JOIN
  {{ ref("diretorio_operadoras") }} AS do
ON
  i.id_operadora = do.id_operadora_jae
LEFT JOIN
  {{ ref("diretorio_consorcios") }} AS dc
ON
  i.id_consorcio = dc.id_consorcio_jae
WHERE i.id_transacao IS NOT NULL