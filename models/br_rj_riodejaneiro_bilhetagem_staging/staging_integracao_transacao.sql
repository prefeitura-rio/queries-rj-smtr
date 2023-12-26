{{
  config(
      alias='integracao_transacao',
  )
}}

SELECT
  data,
  SAFE_CAST(id AS STRING) AS id,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo") AS timestamp_captura,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SAFE_CAST(JSON_VALUE(content, '$.data_inclusao') AS STRING)), 'America/Sao_Paulo') AS data_inclusao,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SAFE_CAST(JSON_VALUE(content, '$.data_processamento') AS STRING)), 'America/Sao_Paulo') AS data_processamento,
  -- Seleciona colunas com os dados de cada transação da integração com os tipos adequados com base no dicionario de parametros
  {% for column, column_config in var('colunas_integracao').items() %}
    {% for i in range(var('quantidade_integracoes_max')) %}
      {% if column_config.type == 'DATETIME' %}
        DATETIME(
          PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            SAFE_CAST(JSON_VALUE(content, '$.{{ column }}_t{% if i > 0 %}i{% endif %}{{ i }}') AS STRING)
          ),
          'America/Sao_Paulo') AS {{ column }}_t{{ i }},
      {% elif column_config.type == 'ID' %}
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.{{ column }}_t{% if i > 0 %}i{% endif %}{{ i }}') AS STRING), '.0', '') AS {{ column }}_t{{ i }},
      {% else %}
        SAFE_CAST(JSON_VALUE(content, '$.{{ column }}_t{% if i > 0 %}i{% endif %}{{ i }}') AS {{ column_config.type }}) AS {{ column }}_t{{ i }},
      {% endif %}
    {% endfor %}
  {% endfor %}
  SAFE_CAST(JSON_VALUE(content, '$.id_status_integracao') AS STRING) AS id_status_integracao,
  SAFE_CAST(JSON_VALUE(content, '$.valor_transacao_total') AS FLOAT64) AS valor_transacao_total,
  SAFE_CAST(JSON_VALUE(content, '$.tx_adicional') AS STRING) AS tx_adicional
FROM
  {{ source('br_rj_riodejaneiro_bilhetagem_staging', 'integracao_transacao') }}