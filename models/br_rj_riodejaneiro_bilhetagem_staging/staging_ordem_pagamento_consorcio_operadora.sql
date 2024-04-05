{{
  config(
    alias='ordem_pagamento_consorcio_operadora',
  )
}}

WITH ordem_pagamento_consorcio_operadora AS (
  SELECT
    data,
    SAFE_CAST(id AS STRING) AS id_ordem_pagamento_consorcio_operadora,
    timestamp_captura,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SAFE_CAST(JSON_VALUE(content, '$.data_inclusao') AS STRING)), "America/Sao_Paulo") AS datetime_inclusao,
    PARSE_DATE('%Y-%m-%d', SAFE_CAST(JSON_VALUE(content, '$.data_ordem') AS STRING)) AS data_ordem,
    SAFE_CAST(JSON_VALUE(content, '$.id_consorcio') AS STRING) AS id_consorcio,
    SAFE_CAST(JSON_VALUE(content, '$.id_operadora') AS STRING) AS id_operadora,
    SAFE_CAST(JSON_VALUE(content, '$.id_ordem_pagamento_consorcio') AS STRING) AS id_ordem_pagamento_consorcio,
    SAFE_CAST(JSON_VALUE(content, '$.qtd_debito') AS INTEGER) AS qtd_debito,
    SAFE_CAST(JSON_VALUE(content, '$.qtd_gratuidade') AS INTEGER) AS qtd_gratuidade,
    SAFE_CAST(JSON_VALUE(content, '$.qtd_integracao') AS INTEGER) AS qtd_integracao,
    SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_credito') AS INTEGER) AS qtd_rateio_credito,
    SAFE_CAST(JSON_VALUE(content, '$.qtd_rateio_debito') AS INTEGER) AS qtd_rateio_debito,
    SAFE_CAST(JSON_VALUE(content, '$.qtd_vendaabordo') AS INTEGER) AS qtd_vendaabordo,
    SAFE_CAST(JSON_VALUE(content, '$.valor_bruto') AS FLOAT64) AS valor_bruto,
    SAFE_CAST(JSON_VALUE(content, '$.valor_debito') AS FLOAT64) AS valor_debito,
    SAFE_CAST(JSON_VALUE(content, '$.valor_gratuidade') AS FLOAT64) AS valor_gratuidade,
    SAFE_CAST(JSON_VALUE(content, '$.valor_integracao') AS FLOAT64) AS valor_integracao,
    SAFE_CAST(JSON_VALUE(content, '$.valor_liquido') AS FLOAT64) AS valor_liquido,
    SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_credito') AS FLOAT64) AS valor_rateio_credito,
    SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_debito') AS FLOAT64) AS valor_rateio_debito,
    SAFE_CAST(JSON_VALUE(content, '$.valor_taxa') AS FLOAT64) AS valor_taxa,
    SAFE_CAST(JSON_VALUE(content, '$.valor_vendaabordo') AS FLOAT64) AS valor_vendaabordo
  FROM
      {{ source("br_rj_riodejaneiro_bilhetagem_staging", "ordem_pagamento_consorcio_operadora") }}
),
ordem_pagamento_consorcio_operadora_rn AS (
  SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id_ordem_pagamento_consorcio_operadora ORDER BY timestamp_captura DESC) AS rn
  FROM
      ordem_pagamento_consorcio_operadora
)
SELECT
  * EXCEPT(rn)
FROM
  ordem_pagamento_consorcio_operadora_rn
WHERE
  rn = 1