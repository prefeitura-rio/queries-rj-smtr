-- Dado teste para comparação de valores
SELECT
  20001 AS agency_id, -- TODO: puxar do GTFS
  EXTRACT(DATE FROM datetime_transacao) AS data,
  EXTRACT(DATE FROM datetime_processamento) AS datetime_processamento,
  COUNT(*) AS n_transacoes,
  MAX(valor_transacao) AS valor_bilhete,
  ROUND(SUM(valor_transacao), 2) AS valor_bruto,
  NULL AS tarifa_cbd,
  NULL AS valor_liquido
FROM
  {{ ref("transacao") }}
WHERE
  datetime_transacao >= "2023-08-01"
GROUP BY
  1,
  2,
  3
ORDER BY
  1,
  2,
  3