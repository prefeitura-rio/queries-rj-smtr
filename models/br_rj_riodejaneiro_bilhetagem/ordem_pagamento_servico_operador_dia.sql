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

WITH ordem_pagamento AS (
    SELECT
        r.data_ordem,
        dc.id_consorcio,
        dc.consorcio,
        do.id_operadora,
        do.operadora,
        r.id_linha AS id_servico_jae,
        s.servico,
        r.id_ordem_pagamento AS id_ordem_pagamento,
        r.id_ordem_ressarcimento AS id_ordem_ressarcimento,
        r.qtd_debito AS quantidade_transacao_debito,
        r.valor_debito,
        r.qtd_vendaabordo AS quantidade_transacao_especie,
        r.valor_vendaabordo AS valor_especie,
        r.qtd_gratuidade AS quantidade_transacao_gratuidade,
        r.valor_gratuidade,
        r.qtd_integracao AS quantidade_transacao_integracao,
        r.valor_integracao,
        COALESCE(rat.qtd_rateio_compensacao_credito_total, r.qtd_rateio_credito) AS quantidade_transacao_rateio_credito,
        COALESCE(rat.valor_rateio_compensacao_credito_total, r.valor_rateio_credito) AS valor_rateio_credito,
        COALESCE(rat.qtd_rateio_compensacao_debito_total, r.qtd_rateio_debito) AS quantidade_transacao_rateio_debito,
        COALESCE(rat.valor_rateio_compensacao_debito_total, r.valor_rateio_debito) AS valor_rateio_debito,
        (
            r.qtd_debito
            + r.qtd_vendaabordo 
            + r.qtd_gratuidade
            + r.qtd_integracao
        ) AS quantidade_total_transacao,
        r.valor_bruto AS valor_total_transacao_bruto,
        r.valor_taxa AS valor_desconto_taxa,
        r.valor_liquido AS valor_total_transacao_liquido
    FROM 
        {{ ref("staging_ordem_ressarcimento") }} r
    LEFT JOIN
        {{ ref("staging_ordem_rateio") }} rat
    USING(data_ordem, id_consorcio, id_operadora, id_linha)
    LEFT JOIN
        {{ ref("operadoras") }} AS do
    ON
        r.id_operadora = do.id_operadora_jae
    LEFT JOIN
        {{ ref("consorcios") }} AS dc
    ON
        r.id_consorcio = dc.id_consorcio_jae
    LEFT JOIN
        {{ ref("servicos") }} AS s
    ON
        r.id_linha = s.id_servico_jae
    {% if is_incremental() %}
        WHERE
            DATE(r.data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
    {% endif %}
)
SELECT
    o.data_ordem,
    o.id_consorcio,
    o.consorcio,
    o.id_operadora,
    o.operadora,
    o.id_servico_jae,
    o.servico,
    o.id_ordem_pagamento,
    o.id_ordem_ressarcimento,
    o.quantidade_transacao_debito,
    o.valor_debito,
    o.quantidade_transacao_especie,
    o.valor_especie,
    o.quantidade_transacao_gratuidade,
    o.valor_gratuidade,
    o.quantidade_transacao_integracao,
    o.valor_integracao,
    o.quantidade_transacao_rateio_credito,
    o.valor_rateio_credito,
    o.quantidade_transacao_rateio_debito,
    o.valor_rateio_debito,
    o.quantidade_total_transacao,
    o.valor_total_transacao_bruto + o.valor_rateio_debito + o.valor_rateio_credito AS valor_total_transacao_bruto,
    o.valor_desconto_taxa,
    o.valor_total_transacao_liquido + o.valor_rateio_debito + o.valor_rateio_credito AS valor_total_transacao_liquido,
    '{{ var("version") }}' AS versao
FROM
    ordem_pagamento o
