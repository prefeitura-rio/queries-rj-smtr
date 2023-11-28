{{
  config(
    materialized="incremental",
    partition_by={

      "field":"data_ordem",
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
    cd_operadora,
    permissao,
    empresa,
    cd_linha,
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
    cd_operadora,
    permissao,
    empresa,
    cd_linha,
    servico
)
SELECT
  COALESCE(t.data_ordem, o.data_ordem) AS data_ordem,
  t.data AS data_processamento_transacao,
  COALESCE(t.consorcio, o.consorcio) AS consorcio,
  COALESCE(t.permissao, o.permissao) AS permissao,
  COALESCE(t.empresa, o.empresa) AS empresa,
  COALESCE(t.servico, o.servico) AS servico,
  o.id_ordem_pagamento,
  o.id_ordem_ressarcimento,
  o.quantidade_transacao_total AS quantidade_total_ordem,
  o.valor_transacao_total_bruto AS valor_total_ordem,
  t.quantidade_total_captura,
  t.valor_total_captura,
  '{{ var("version") }}' AS versao
FROM
  transacao_agg AS t
FULL OUTER JOIN
    {{ ref("ordem_pagamento") }} o
ON
    (t.data_ordem = o.data_ordem)
    AND (t.cd_linha = o.cd_linha)
    AND (t.cd_operadora = o.cd_operadora)

