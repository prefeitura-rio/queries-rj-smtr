{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
    unique_key=["id_ordem_pagamento"],
    incremental_strategy="insert_overwrite"
  )
}}

SELECT
    r.data_ordem AS data,
    p.data_pagamento,
    c.nm_consorcio AS consorcio,
    CASE
        WHEN r.id_operadora = "1" THEN "22.100005-0"
    END AS permissao,
    pj.nm_fantasia AS empresa,
    l.nr_linha AS servico,
    r.qtd_debito AS quantidade_transacao_debito,
    r.valor_debito,
    r.qtd_vendaabordo AS quantidade_transacao_especie,
    r.valor_vendaabordo AS valor_especie,
    r.qtd_gratuidade AS quantidade_transacao_gratuidade,
    r.valor_gratuidade,
    r.qtd_integracao AS quantidade_transacao_integracao,
    r.valor_integracao,
    r.qtd_rateio_credito AS quantidade_transacao_rateio_credito,
    r.valor_rateio_credito,
    r.qtd_rateio_debito AS quantidade_transacao_rateio_debito,
    r.valor_rateio_debito,
    r.qtd_debito +  r.qtd_vendaabordo +  r.qtd_gratuidade + r.qtd_integracao + r.qtd_rateio_credito + r.qtd_rateio_debito AS quantidade_total_transacao,
    r.valor_bruto AS valor_total_transacao,
    r.valor_taxa AS valor_desconto_taxa,
    r.valor_liquido AS valor_total_liquido,
    '{{ var("version") }}' as versao
FROM 
    {{ ref("staging_ordem_ressarcimento") }} r
LEFT JOIN
    {{ ref("staging_ordem_pagamento") }} p
ON
    r.id_ordem_pagamento = p.id_ordem_pagamento
LEFT JOIN
    {{ ref("staging_consorcio") }} c
ON
    r.id_consorcio = c.cd_consorcio
LEFT JOIN
    {{ ref("staging_linha") }} AS l
ON
    r.id_linha = l.cd_linha
LEFT JOIN
    {{ ref("staging_operadora_transporte") }} AS o
ON
    r.id_operadora = o.cd_operadora_transporte
LEFT JOIN
    {{ ref("staging_pessoa_juridica") }} AS pj
ON
    o.cd_cliente = pj.cd_cliente
{% if is_incremental() -%}
  WHERE
    r.timestamp_captura BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
{%- endif %}