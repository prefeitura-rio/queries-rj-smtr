{{
  config(
    incremental_strategy="insert_overwrite",
    partition_by={
      "field": "data_ordem", 
      "data_type": "date",
      "granularity": "day"
    },
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
        do.id_operadora,
        t.valor_transacao,
        t.tipo_transacao,
        t.id_tipo_modal,
        dc.id_consorcio,
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
    LEFT JOIN
      {{ ref("operadoras") }} AS do
    ON
      t.cd_operadora = do.id_operadora_jae
    LEFT JOIN
      {{ ref("consorcios") }} AS dc
    ON
      t.cd_consorcio = dc.id_consorcio_jae
    WHERE
        {% if is_incremental() %}
            DATE(t.data) BETWEEN DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 2 DAY) AND DATE("{{var('run_date')}}")
            AND t.data_processamento BETWEEN DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 2 DAY) AND DATE("{{var('run_date')}}")
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
        ANY_VALUE(t.id_consorcio) AS id_consorcio,
        t.cd_linha AS id_servico_jae,
        t.id_operadora,
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
            t.data_ordem <= DATE("{{var('run_date')}}")
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
        t.id_operadora
),
ordem_pagamento AS (
  SELECT
    *
  FROM
    {{ ref("ordem_pagamento_servico_operador_dia") }}
  {% if is_incremental() %}
    WHERE
      data_ordem = DATE("{{var('run_date')}}")
  {% endif %}
),
id_ordem_pagamento AS (
  SELECT
    data_ordem,
    id_ordem_pagamento
  FROM
    {{ ref("ordem_pagamento_dia") }}
  {% if is_incremental() %}
    WHERE
      data_ordem = DATE("{{var('run_date')}}")
  {% endif %}
),
transacao_ordem AS (
  SELECT
    COALESCE(op.data_ordem, t.data_ordem) AS data_ordem,
    COALESCE(op.id_consorcio, t.id_consorcio) AS id_consorcio,
    COALESCE(op.id_operadora, t.id_operadora) AS id_operadora,
    COALESCE(op.id_servico_jae, t.id_servico_jae) AS id_servico_jae,
    op.quantidade_total_transacao,
    op.valor_total_transacao_bruto,
    op.valor_total_transacao_liquido,
    t.quantidade_total_transacao_captura,
    CAST(t.valor_total_transacao_captura + op.valor_rateio_credito + op.valor_rateio_debito AS NUMERIC) AS valor_total_transacao_captura
  FROM
    ordem_pagamento op
  FULL OUTER JOIN
    transacao_agg t
  USING(data_ordem, id_servico_jae, id_operadora)
)
SELECT
  o.data_ordem,
  id.id_ordem_pagamento,
  o.id_consorcio,
  o.id_operadora,
  o.id_servico_jae,
  o.quantidade_total_transacao,
  o.valor_total_transacao_bruto,
  o.valor_total_transacao_liquido,
  o.quantidade_total_transacao_captura,
  o.valor_total_transacao_captura,
  COALESCE(
    (
      quantidade_total_transacao_captura != quantidade_total_transacao 
      OR ROUND(valor_total_transacao_captura, 2) != ROUND(valor_total_transacao_bruto, 2)
    ),
    TRUE
  ) AS indicador_captura_invalida,
  '{{ var("version") }}' AS versao
FROM
  transacao_ordem o
JOIN
  id_ordem_pagamento id
USING(data_ordem)
  