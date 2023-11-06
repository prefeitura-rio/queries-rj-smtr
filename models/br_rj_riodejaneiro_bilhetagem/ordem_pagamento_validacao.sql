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
    data,
    DATE_ADD(data, INTERVAL 1 DAY) AS data_ordem,
    consorcio,
    permissao,
    empresa,
    servico,
    COUNT(*) AS quantidade_total_captura,
    SUM(valor_transacao) AS valor_total_captura,
  FROM
    {{ ref("transacao") }}
  {% if is_incremental() -%}
  WHERE
    data BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
  {%- endif %}
  GROUP BY
    data,
    consorcio,
    permissao,
    empresa,
    servico
)
SELECT
  transacao_agg.data AS data,
  transacao_agg.data_ordem AS data_ordem,
  transacao_agg.consorcio AS consorcio,
  transacao_agg.permissao AS permissao,
  transacao_agg.empresa AS empresa,
  transacao_agg.servico AS servico,
  o.quantidade_total_transacao AS quantidade_total_ordem,
  o.valor_total_transacao AS valor_total_ordem,
  transacao_agg.quantidade_total_captura AS quantidade_total_captura,
  transacao_agg.valor_total_captura AS valor_total_captura,
  '{{ var("version") }}' as versao
FROM
  transacao_agg
LEFT JOIN
    {{ ref("ordem_pagamento") }} o
ON
    (transacao_agg.data_ordem = o.data)
    AND (transacao_agg.servico = o.servico)
    AND (transacao_agg.consorcio = o.consorcio)
    AND (transacao_agg.permissao = o.permissao)
    AND (transacao_agg.empresa = o.empresa)