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

WITH ordem_pagamento_consorcio_operador_dia AS (
  SELECT
    data_ordem,
    id_consorcio,
    id_ordem_pagamento,
    SUM(quantidade_total_transacao) AS quantidade_total_transacao,
    SUM(valor_total_transacao_liquido) AS valor_total_transacao_liquido,
    MAX(indicador_captura_invalida) AS indicador_captura_invalida
  FROM
    {{ ref("ordem_pagamento_consorcio_operador_dia_validacao") }}
  {% if is_incremental() %}
    WHERE
      data_ordem = DATE("{{var('run_date')}}")
  {% endif %}
  GROUP BY
    1,
    2,
    3
),
ordem_pagamento_consorcio_dia AS (
  SELECT
    data_ordem,
    id_consorcio,
    id_ordem_pagamento,
    quantidade_total_transacao,
    valor_total_transacao_liquido
  FROM
    {{ ref("ordem_pagamento_consorcio_dia") }}
  {% if is_incremental() %}
    WHERE
      data_ordem = DATE("{{var('run_date')}}") 
  {% endif %}
)
SELECT
  cd.data_ordem,
  cd.id_consorcio,
  cd.id_ordem_pagamento,
  cd.quantidade_total_transacao,
  cd.valor_total_transacao_liquido,
  cd.indicador_captura_invalida,
  ROUND(cd.valor_total_transacao_liquido, 2) != ROUND(cod.valor_total_transacao_liquido, 2) OR cd.quantidade_total_transacao != cod.quantidade_total_transacao AS indicador_agregacao_invalida,
  '{{ var("version") }}' AS versao
FROM
  ordem_pagamento_consorcio_dia cd
LEFT JOIN
  ordem_pagamento_consorcio_operador_dia cod
USING(
  data_ordem,
  id_consorcio,
  id_ordem_pagamento
)
