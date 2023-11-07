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
    cd_cliente,
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
    cd_cliente,
    permissao,
    empresa,
    cd_linha,
    servico
)
SELECT
  transacao_agg.data AS data,
  COALESCE(transacao_agg.data_ordem, o.data) AS data_ordem,
  o.id_ordem_pagamento as id_ordem_pagamento,
  o.id_ordem_ressarcimento as id_ordem_ressarcimento,
  COALESCE(transacao_agg.consorcio, o.consorcio) AS consorcio,
  COALESCE(transacao_agg.permissao, o.permissao) AS permissao,
  COALESCE(transacao_agg.empresa, o.empresa) AS empresa,
  COALESCE(transacao_agg.servico, o.servico) AS servico,
  o.quantidade_total_transacao AS quantidade_total_ordem,
  o.valor_total_transacao AS valor_total_ordem,
  transacao_agg.quantidade_total_captura AS quantidade_total_captura,
  transacao_agg.valor_total_captura AS valor_total_captura,
  '{{ var("version") }}' as versao
FROM
  transacao_agg
FULL OUTER JOIN
    {{ ref("ordem_pagamento") }} o
ON
    (transacao_agg.data_ordem = o.data)
    AND (transacao_agg.cd_linha = o.id_linha)
    AND (transacao_agg.cd_cliente = o.cd_cliente)