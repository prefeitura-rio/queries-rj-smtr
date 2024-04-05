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

-- WITH servico_motorista AS (
--     SELECT
--         * EXCEPT(rn)
--     FROM
--     (
--         SELECT
--             id_servico,
--             dt_fechamento,
--             nr_logico_midia,
--             cd_linha,
--             cd_operadora,
--             ROW_NUMBER() OVER (PARTITION BY id_servico, nr_logico_midia ORDER BY timestamp_captura DESC) AS rn
--         FROM
--             {{ ref("staging_servico_motorista") }}
--         {% if is_incremental() %}
--             WHERE
--                 DATE(data) BETWEEN DATE_SUB(DATE("{{var('date_range_start')}}"), INTERVAL 1 DAY) AND DATE("{{var('date_range_end')}}")
--         {% endif %}  
--     )   
-- ),
WITH transacao AS (
    SELECT
        t.id,
        t.timestamp_captura,
        DATE(t.data_processamento) AS data_processamento,
        t.data_processamento AS datetime_processamento,
        t.cd_linha,
        t.cd_operadora,
        t.valor_transacao,
        t.tipo_transacao,
        t.id_tipo_modal,
        t.cd_consorcio,
        -- sm.dt_fechamento AS datetime_fechamento_servico,
        -- sm.cd_linha AS cd_linha_servico,
        -- sm.cd_operadora AS cd_operadora_servico,
        t.id_servico
    FROM
        {{ ref("staging_transacao") }} t
    -- LEFT JOIN
    --     servico_motorista sm
    -- ON
    --     sm.id_servico = t.id_servico
    --     AND sm.nr_logico_midia = t.nr_logico_midia_operador
    WHERE
        {% if is_incremental() %}
            DATE(t.data) BETWEEN DATE_SUB(DATE("{{var('date_range_start')}}"), INTERVAL 1 DAY) AND DATE_ADD(DATE("{{var('date_range_end')}}"), INTERVAL 1 DAY)
            AND t.data_processamento BETWEEN DATE_SUB(DATE("{{var('date_range_start')}}"), INTERVAL 1 DAY) AND DATE_ADD(DATE("{{var('date_range_end')}}"), INTERVAL 1 DAY)
        {% else %}
            DATE(t.data) <= CURRENT_DATE("America/Sao_Paulo")
            AND DATE(t.data_processamento) <= CURRENT_DATE("America/Sao_Paulo")
        {% endif %}
),
transacao_deduplicada AS (
    SELECT 
        t.* EXCEPT(rn),
        DATE_ADD(data_processamento, INTERVAL 1 DAY) AS data_ordem -- TODO: Regra da data por serviços fechados no modo Ônibus quando começar a operação
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
        FROM
            transacao
    ) t
    WHERE
        rn = 1
),
transacao_agg AS (
    SELECT
        t.data_ordem,
        ANY_VALUE(t.cd_consorcio) AS cd_consorcio,
        t.cd_linha,
        t.cd_operadora,
        COUNT(*) AS quantidade_total_transacao_captura,
        SUM(t.valor_transacao) AS valor_total_transacao_captura
    FROM
        transacao_deduplicada t
    LEFT JOIN
        {{ ref("staging_linha_sem_ressarcimento") }} l
    ON
        t.cd_linha = l.id_linha
    WHERE
        -- Remove dados com data de ordem de pagamento maiores que a execução do modelo
        {% if is_incremental() %}
            t.data_ordem <= DATE("{{var('date_range_end')}}")
        {% else %}
            t.data_ordem <= CURRENT_DATE("America/Sao_Paulo")
        {% endif %}
        -- Remove linhas de teste que não entram no ressarcimento
        AND l.id_linha IS NULL
        -- Remove gratuidades e transferências da contagem de transações
        AND tipo_transacao NOT IN ('5', '21')
    GROUP BY
        t.data_ordem,
        t.cd_linha,
        t.cd_operadora
),
ordem_pagamento AS (
    SELECT
        r.data_ordem,
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
        COALESCE(rat.qtd_rateio_compensacao_credito_total, r.qtd_rateio_credito) AS quantidade_transacao_rateio_credito,
        COALESCE(rat.valor_rateio_compensacao_credito_total, r.valor_rateio_credito) AS valor_rateio_credito,
        COALESCE(rat.qtd_rateio_compensacao_debito_total, r.qtd_rateio_debito) AS quantidade_transacao_rateio_debito,
        COALESCE(rat.valor_rateio_compensacao_debito_total, r.valor_rateio_debito) AS valor_rateio_debito,
        r.valor_bruto AS valor_total_transacao_bruto,
        r.valor_taxa AS valor_desconto_taxa,
        r.valor_liquido AS valor_total_transacao_liquido
    FROM 
        {{ ref("staging_ordem_ressarcimento") }} r
    LEFT JOIN
        {{ ref("staging_ordem_rateio") }} rat
    USING(data_ordem, id_consorcio, id_operadora, id_linha)
    {% if is_incremental() %}
        WHERE
            DATE(r.data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
    {% endif %}
),
ordem_pagamento_transacao AS (
    SELECT
        COALESCE(op.data_ordem, t.data_ordem) AS data_ordem,
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
        (
            op.quantidade_transacao_debito
            + op.quantidade_transacao_especie 
            + op.quantidade_transacao_gratuidade
            + op.quantidade_transacao_integracao
        ) AS quantidade_total_transacao,
        op.valor_total_transacao_bruto + op.valor_rateio_debito + op.valor_rateio_credito AS valor_total_transacao_bruto,
        op.valor_total_transacao_liquido + op.valor_rateio_debito + op.valor_rateio_credito AS valor_total_transacao_liquido,
        op.valor_desconto_taxa,
        t.quantidade_total_transacao_captura,
        t.valor_total_transacao_captura + op.valor_rateio_credito + op.valor_rateio_debito AS valor_total_transacao_captura
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
    dc.id_consorcio,
    dc.consorcio,
    do.id_operadora,
    do.operadora,
    o.id_linha AS id_servico_jae,
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
    valor_total_transacao_bruto,
    o.valor_desconto_taxa,
    o.valor_total_transacao_liquido,
    o.quantidade_total_transacao_captura,
    o.valor_total_transacao_captura,
    COALESCE(
        (
            o.quantidade_total_transacao_captura = o.quantidade_total_transacao 
            AND ROUND(o.valor_total_transacao_captura, 2) = ROUND(o.valor_total_transacao_bruto, 2)
        ),
        FALSE
    ) AS indicador_ordem_valida,
    '{{ var("version") }}' AS versao
FROM
    ordem_pagamento_transacao o
LEFT JOIN
    {{ ref("operadoras") }} AS do
ON
    o.id_operadora = do.id_operadora_jae
LEFT JOIN
    {{ ref("consorcios") }} AS dc
ON
    o.id_consorcio = dc.id_consorcio_jae
