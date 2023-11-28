{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data_ordem",
      "data_type":"date",
      "granularity": "day"
    },
    unique_key=["id_ordem_pagamento"],
    incremental_strategy="insert_overwrite"
  )
}}

WITH transacao_agg AS (
    SELECT
        data,
        DATE_ADD(data, INTERVAL 1 DAY) AS data_ordem,
        ANY_VALUE(consorcio) AS consorcio,
        cd_operadora,
        cd_linha,
        ANY_VALUE(permissao) AS permissao,
        ANY_VALUE(empresa) AS empresa,
        ANY_VALUE(servico) AS servico,
        COUNT(*) AS quantidade_total_captura,
        ROUND(SUM(valor_transacao), 1) AS valor_total_captura
    FROM
        {{ ref("transacao") }}
    WHERE
        {% if is_incremental() -%}
            data BETWEEN DATE_SUB(DATE("{{var('date_range_start')}}"), INTERVAL 1 DAY) AND DATE_SUB(DATE("{{var('date_range_end')}}"), INTERVAL 1 DAY)
        {% else %}
            data < CURRENT_DATE("America/Sao_Paulo")
        {%- endif %}
    GROUP BY
        data,
        cd_operadora,
        cd_linha
),
ordem_pagamento AS (
    SELECT
        r.data_ordem,
        p.data_pagamento,
        c.nm_consorcio AS consorcio,
        r.id_operadora AS cd_operadora,
        CASE
            WHEN r.id_operadora = "1" THEN "22.100005-0"
        END AS permissao,
        pj.nm_fantasia AS empresa,
        r.id_linha AS cd_linha,
        l.nr_linha AS servico,
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
        r.qtd_rateio_credito AS quantidade_transacao_rateio_credito,
        r.valor_rateio_credito,
        r.qtd_rateio_debito AS quantidade_transacao_rateio_debito,
        r.valor_rateio_debito,
        r.qtd_debito +  r.qtd_vendaabordo +  r.qtd_gratuidade + r.qtd_integracao + r.qtd_rateio_credito + r.qtd_rateio_debito AS quantidade_transacao_total,
        ROUND(r.valor_bruto, 1) AS valor_transacao_total_bruto,
        r.valor_taxa AS valor_desconto_taxa,
        r.valor_liquido AS valor_transacao_total_liquido
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
        DATE(r.data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
    {%- endif %}
)
SELECT
    COALESCE(op.data_ordem, t.data_ordem) AS data_ordem,
    op.data_pagamento,
    COALESCE(op.consorcio, t.consorcio) AS consorcio,
    COALESCE(op.cd_operadora, t.cd_operadora) AS cd_operadora,
    COALESCE(op.permissao, t.permissao) AS permissao,
    COALESCE(op.empresa, t.empresa) AS empresa,
    COALESCE(op.cd_linha, t.cd_linha) AS cd_linha,
    COALESCE(op.servico, t.servico) AS servico,
    op.id_ordem_pagamento,
    op.id_ordem_ressarcimento,
    op.quantidade_transacao_debito,
    op.valor_debito,
    op.quantidade_transacao_especie,
    op.valor_especie,
    op.quantidade_transacao_gratuidade,
    op.valor_gratuidade,
    op.quantidade_transacao_integracao,
    op.valor_integracao,
    op.quantidade_transacao_rateio_credito,
    op.valor_rateio_credito,
    op.quantidade_transacao_rateio_debito,
    op.valor_rateio_debito,
    op.quantidade_transacao_total,
    op.valor_transacao_total_bruto,
    op.valor_desconto_taxa,
    op.valor_transacao_total_liquido,
    t.quantidade_total_captura,
    t.valor_total_captura,
    COALESCE(
        (t.quantidade_total_captura = op.quantidade_transacao_total AND t.valor_total_captura = op.valor_transacao_total_bruto),
        false
    ) AS flag_valido,
    '{{ var("version") }}' AS versao
FROM
    ordem_pagamento op
FULL OUTER JOIN
    transacao_agg t
ON
    t.data_ordem = op.data_ordem
    AND t.cd_linha = op.cd_linha
    AND t.cd_operadora = op.cd_operadora
