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
WITH transacao AS (
    SELECT
        id,
        timestamp_captura,
        DATE(data_processamento) AS data_processamento,
        cd_linha,
        cd_operadora,
        valor_transacao,
        tipo_transacao,
        cd_consorcio
    FROM
        {{ ref("staging_transacao") }}
    WHERE
        {% if is_incremental() -%}
            DATE(data) BETWEEN DATE_SUB(DATE("{{var('date_range_start')}}"), INTERVAL 1 DAY) AND DATE("{{var('date_range_end')}}")
        {% else %}
            DATE(data) <= CURRENT_DATE("America/Sao_Paulo")
        {%- endif %}
),
transacao_deduplicada AS (
    SELECT 
        t.* EXCEPT(rn)
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
        FROM
            transacao
    ) t
    WHERE
        -- Remover gratuidades da contagem de transações
        tipo_transacao != '21'
        AND rn = 1
        AND
        {% if is_incremental() -%}
            t.data_processamento < DATE("{{var('date_range_end')}}")
        {% else %}
            t.data_processamento < CURRENT_DATE("America/Sao_Paulo")
        {%- endif %}
        
),
transacao_agg AS (
    SELECT
        data_processamento,
        DATE_ADD(data_processamento, INTERVAL 1 DAY) AS data_ordem,
        ANY_VALUE(cd_consorcio) AS cd_consorcio,
        cd_linha,
        cd_operadora,
        COUNT(*) AS quantidade_total_transacao_captura,
        ROUND(SUM(valor_transacao), 2) AS valor_total_transacao_captura
    FROM
        transacao_deduplicada
    GROUP BY
        data_processamento,
        cd_linha,
        cd_operadora
),
ordem_pagamento AS (
    SELECT
        DATE_SUB(r.data_ordem, INTERVAL 1 DAY) AS data_processamento,
        r.data_ordem,
        p.data_pagamento,
        r.id_consorcio,
        r.id_operadora,
        r.id_linha,
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
        r.qtd_debito +  r.qtd_vendaabordo +  r.qtd_gratuidade + r.qtd_integracao + r.qtd_rateio_credito + r.qtd_rateio_debito AS quantidade_total_transacao,
        ROUND(r.valor_bruto, 2) AS valor_total_transacao_bruto,
        r.valor_taxa AS valor_desconto_taxa,
        r.valor_liquido AS valor_total_transacao_liquido
    FROM 
        {{ ref("staging_ordem_ressarcimento") }} r
    LEFT JOIN
        {{ ref("staging_ordem_pagamento") }} p
    ON
        r.id_ordem_pagamento = p.id_ordem_pagamento
    {% if is_incremental() -%}
        WHERE
            DATE(r.data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
    {%- endif %}
),
ordem_pagamento_validacao AS (
    SELECT
        COALESCE(op.data_processamento, t.data_processamento) AS data_processamento,
        COALESCE(op.data_ordem, t.data_ordem) AS data_ordem,
        op.data_pagamento,
        COALESCE(op.id_consorcio, t.cd_consorcio) AS id_consorcio,
        COALESCE(op.id_operadora, t.cd_operadora) AS id_operadora,
        COALESCE(op.id_linha, t.cd_linha) AS id_linha,
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
        op.quantidade_total_transacao,
        op.valor_total_transacao_bruto,
        op.valor_desconto_taxa,
        op.valor_total_transacao_liquido,
        t.quantidade_total_transacao_captura,
        t.valor_total_transacao_captura,
        COALESCE(
            (t.quantidade_total_transacao_captura = op.quantidade_total_transacao AND t.valor_total_transacao_captura = op.valor_total_transacao_bruto),
            false
        ) AS indicador_ordem_valida
    FROM
        ordem_pagamento op
    FULL OUTER JOIN
        transacao_agg t
    ON
        t.data_ordem = op.data_ordem
        AND t.cd_linha = op.id_linha
        AND t.cd_operadora = op.id_operadora
)
SELECT
    o.data_ordem,
    o.data_pagamento,
    dc.id_consorcio,
    dc.consorcio,
    do.id_operadora,
    do.operadora,
    l.nr_linha AS servico,
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
    o.valor_total_transacao_bruto,
    o.valor_desconto_taxa,
    o.valor_total_transacao_liquido,
    o.quantidade_total_transacao_captura,
    o.valor_total_transacao_captura,
    o.indicador_ordem_valida,
    '{{ var("version") }}' AS versao
FROM
    ordem_pagamento_validacao o
LEFT JOIN
    {{ ref("diretorio_operadoras") }} AS do
ON
    o.id_operadora = do.id_operadora_jae
LEFT JOIN
    {{ ref("diretorio_consorcios") }} AS dc
ON
    o.id_consorcio = dc.id_consorcio_jae
LEFT JOIN
    {{ ref("staging_linha") }} AS l
ON
    o.id_linha = l.cd_linha 
    AND o.data_processamento >= l.datetime_inclusao