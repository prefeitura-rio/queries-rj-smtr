{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
    unique_key=["data", "servico", "consorcio", "empresa"],
    incremental_strategy="insert_overwrite"
  )
}}

WITH
  transacao_agg AS (
  SELECT
    EXTRACT(DATE FROM data_processamento) AS data,
    servico,
    consorcio,
    permissao,
    empresa,
    COUNT(*) AS quantidade_total_captura,
    SUM(valor_transacao) AS valor_total_captura
  FROM
    {{ ref("transacao") }}
  GROUP BY
    EXTRACT(DATE FROM data_processamento),
    servico,
    consorcio,
    permissao,
    empresa
)
SELECT
  transacao_agg.data,
  DATE_ADD(transacao_agg.data, INTERVAL 1 DAY) AS data_pagamento,
  transacao_agg.servico,
  transacao_agg.consorcio,
  transacao_agg.permissao,
  transacao_agg.empresa,
  o.quantidade_total_ordem,
  o.valor_total_transacao AS valor_total_ordem,
  transacao_agg.quantidade_total_captura,
  transacao_agg,valor_total_captura
FROM
  transacao_agg
LEFT JOIN
    {{ ref("ordem_pagamento") }} o
ON
    (transacao_agg.data_pagamento = o.data_ordem)
    AND (transacao_agg.servico = o.servico)
    AND (transacao_agg.consorcio = o.consorcio)
    AND (transacao_agg.permissao = o.permissao)
    AND (transacao_agg.empresa = o.empresa)