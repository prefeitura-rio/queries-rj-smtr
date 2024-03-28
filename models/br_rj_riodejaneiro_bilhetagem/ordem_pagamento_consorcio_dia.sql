{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data_ordem",
      "data_type":"date",
      "granularity": "day"
    },
    incremental_strategy="insert_overwrite"
  )
}}

-- depends_on: {{ ref("ordem_pagamento_consorcio_operador_dia") }}
SELECT
    o.data_ordem,
    dc.id_consorcio,
    dc.consorcio,
    o.id_ordem_pagamento AS id_ordem_pagamento,
    o.qtd_debito AS quantidade_transacao_debito,
    o.valor_debito,
    o.qtd_vendaabordo AS quantidade_transacao_especie,
    o.valor_vendaabordo AS valor_especie,
    o.qtd_gratuidade AS quantidade_transacao_gratuidade,
    o.valor_gratuidade,
    o.qtd_integracao AS quantidade_transacao_integracao,
    o.valor_integracao,
    o.qtd_rateio_credito AS quantidade_transacao_rateio_credito,
    o.valor_rateio_credito AS valor_rateio_credito,
    o.qtd_rateio_debito AS quantidade_transacao_rateio_debito,
    o.valor_rateio_debito AS valor_rateio_debito,
    ROUND(o.valor_bruto + 0.0005, 2) AS valor_total_transacao_bruto,
    o.valor_taxa AS valor_desconto_taxa,
    ROUND(o.valor_liquido + 0.0005, 2) AS valor_total_transacao_liquido,
    '{{ var("version") }}' AS versao
FROM 
    {{ ref("staging_ordem_pagamento_consorcio") }} o
LEFT JOIN
    {{ ref("consorcios") }} dc
ON
    o.id_consorcio = dc.id_consorcio_jae
{% if is_incremental() %}
    WHERE
        DATE(o.data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
{% endif %}
