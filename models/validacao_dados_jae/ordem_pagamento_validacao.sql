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

WITH servico_operador_dia_validacao AS (
  SELECT
    data_ordem,
    id_ordem_pagamento,
    MAX(indicador_captura_invalida) AS indicador_servico_operador_invalido,
  FROM
    {{ ref("ordem_pagamento_servico_operador_dia_validacao") }}
  {% if is_incremental() %}
    WHERE
      data_ordem BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
  {% endif %}
  GROUP BY
    1,
    2
),
consorcio_operador_dia_validacao AS (
  SELECT
    data_ordem,
    id_ordem_pagamento,
    MAX(indicador_captura_invalida) OR MAX(indicador_agregacao_invalida) AS indicador_consorcio_operador_invalido,
  FROM
    {{ ref("ordem_pagamento_consorcio_operador_dia_validacao") }}
  {% if is_incremental() %}
    WHERE
      data_ordem BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
  {% endif %}
  GROUP BY
    1,
    2
),
consorcio_dia_validacao AS (
  SELECT
    data_ordem,
    id_ordem_pagamento,
    SUM(quantidade_total_transacao) AS quantidade_total_transacao,
    SUM(valor_total_transacao_liquido) AS valor_total_transacao_liquido,
    MAX(indicador_captura_invalida) OR MAX(indicador_agregacao_invalida) AS indicador_consorcio_invalido
  FROM
    {{ ref("ordem_pagamento_consorcio_dia_validacao") }}
  {% if is_incremental() %}
    WHERE
      data_ordem BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
  {% endif %}
  GROUP BY
    1,
    2
),
ordem_pagamento_dia AS (
  SELECT
    data_ordem,
    id_ordem_pagamento,
    quantidade_total_transacao,
    valor_total_transacao_liquido
  FROM
    {{ ref("ordem_pagamento_dia") }}
  {% if is_incremental() %}
    WHERE
      data_ordem BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
  {% endif %}
),
validacao AS (
  SELECT
    cd.data_ordem,
    cd.id_ordem_pagamento,
    ROUND(cd.valor_total_transacao_liquido, 2) != ROUND(d.valor_total_transacao_liquido, 2) OR cd.quantidade_total_transacao != d.quantidade_total_transacao AS indicador_agregacao_invalida,
    indicador_servico_operador_invalido,
    indicador_consorcio_operador_invalido,
    indicador_consorcio_invalido
  FROM
    ordem_pagamento_dia d
  LEFT JOIN
    consorcio_dia_validacao cd
  ON
    d.data_ordem = cd.data_ordem
    AND d.id_ordem_pagamento = cd.id_ordem_pagamento
  LEFT JOIN
    consorcio_operador_dia_validacao cod
  ON
    d.data_ordem = cod.data_ordem
    AND d.id_ordem_pagamento = cod.id_ordem_pagamento
  LEFT JOIN
    servico_operador_dia_validacao sod
  ON
    d.data_ordem = sod.data_ordem
    AND d.id_ordem_pagamento = sod.id_ordem_pagamento
)
SELECT
  *,
  (
    indicador_agregacao_invalida
    OR indicador_servico_operador_invalido
    OR indicador_consorcio_operador_invalido
    OR indicador_consorcio_invalido
  ) AS indicador_ordem_invalida,
  '{{ var("version") }}' AS versao
FROM
  validacao