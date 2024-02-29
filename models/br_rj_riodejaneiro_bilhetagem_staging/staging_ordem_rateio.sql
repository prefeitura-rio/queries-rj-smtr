{{
  config(
    alias='ordem_rateio',
  )
}}

WITH ordem_rateio AS (
  SELECT
    data,
        SAFE_CAST(id AS STRING) AS id,
        timestamp_captura,
        DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SAFE_CAST(JSON_VALUE(content, '$.data_inclusao') AS STRING)), 'America/Sao_Paulo') AS data_inclusao,
        PARSE_DATE('%Y-%m-%d', SAFE_CAST(JSON_VALUE(content, '$.data_ordem') AS STRING)) AS data_ordem,
        SAFE_CAST(JSON_VALUE(content, '$.id_consorcio') AS STRING) AS id_consorcio,
        SAFE_CAST(JSON_VALUE(content, '$.id_linha') AS STRING) AS id_linha,
        SAFE_CAST(JSON_VALUE(content, '$.id_operadora') AS STRING) AS id_operadora,
        SAFE_CAST(JSON_VALUE(content, '$.id_ordem_pagamento') AS STRING) AS id_ordem_pagamento,
        SAFE_CAST(JSON_VALUE(content, '$.id_ordem_pagamento_consorcio') AS STRING) AS id_ordem_pagamento_consorcio,
        SAFE_CAST(JSON_VALUE(content, '$.id_ordem_pagamento_consorcio_operadora') AS STRING) AS id_ordem_pagamento_consorcio_operadora,
        SAFE_CAST(JSON_VALUE(content, '$.id_status_ordem') AS STRING) AS id_status_ordem,
        SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_compensacao_credito_t0') AS FLOAT64) AS INTEGER) AS qtd_rateio_compensacao_credito_t0,
        SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_compensacao_credito_t1') AS FLOAT64) AS INTEGER) AS qtd_rateio_compensacao_credito_t1,
        SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_compensacao_credito_t2') AS FLOAT64) AS INTEGER) AS qtd_rateio_compensacao_credito_t2,
        SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_compensacao_credito_t3') AS FLOAT64) AS INTEGER) AS qtd_rateio_compensacao_credito_t3,
        SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_compensacao_credito_t4') AS FLOAT64) AS INTEGER) AS qtd_rateio_compensacao_credito_t4,
        SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_compensacao_credito_total') AS FLOAT64) AS INTEGER) AS qtd_rateio_compensacao_credito_total,
        SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_compensacao_debito_t0') AS FLOAT64) AS INTEGER) AS qtd_rateio_compensacao_debito_t0,
        SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_compensacao_debito_t1') AS FLOAT64) AS INTEGER) AS qtd_rateio_compensacao_debito_t1,
        SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_compensacao_debito_t2') AS FLOAT64) AS INTEGER) AS qtd_rateio_compensacao_debito_t2,
        SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_compensacao_debito_t3') AS FLOAT64) AS INTEGER) AS qtd_rateio_compensacao_debito_t3,
        SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_compensacao_debito_t4') AS FLOAT64) AS INTEGER) AS qtd_rateio_compensacao_debito_t4,
        SAFE_CAST(SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_compensacao_debito_total') AS FLOAT64) AS INTEGER) AS qtd_rateio_compensacao_debito_total,
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_credito_t0') AS FLOAT64) AS valor_rateio_compensacao_credito_t0,
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_credito_t1') AS FLOAT64) AS valor_rateio_compensacao_credito_t1,
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_credito_t2') AS FLOAT64) AS valor_rateio_compensacao_credito_t2,
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_credito_t3') AS FLOAT64) AS valor_rateio_compensacao_credito_t3,
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_credito_t4') AS FLOAT64) AS valor_rateio_compensacao_credito_t4,
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_credito_total') AS FLOAT64) AS valor_rateio_compensacao_credito_total,
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_debito_t0') AS FLOAT64) AS valor_rateio_compensacao_debito_t0,
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_debito_t1') AS FLOAT64) AS valor_rateio_compensacao_debito_t1,
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_debito_t2') AS FLOAT64) AS valor_rateio_compensacao_debito_t2,
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_debito_t3') AS FLOAT64) AS valor_rateio_compensacao_debito_t3,
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_debito_t4') AS FLOAT64) AS valor_rateio_compensacao_debito_t4,
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_debito_total') AS FLOAT64) AS valor_rateio_compensacao_debito_total
  FROM
    {{ source('br_rj_riodejaneiro_bilhetagem_staging', 'ordem_rateio') }}
)
SELECT 
  * EXCEPT(rn)
FROM
(
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
  FROM
    ordem_rateio
)
WHERE
  rn = 1
