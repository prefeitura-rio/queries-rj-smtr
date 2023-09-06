-- Dado teste para comparação de valores
SELECT
  20001 AS agency_id, -- TODO: puxar do GTFS
  EXTRACT(DATE FROM data_transacao) AS data,
  EXTRACT(DATE FROM data_processamento) AS data_processamento,
  COUNT(*) AS n_transacoes,
  MAX(valor_tarifa) AS valor_bilhete,
  ROUND(SUM(valor_transacao), 2) AS valor_bruto,
  NULL AS tarifa_cbd,
  NULL AS valor_liquido
FROM
  {{ ref("transacao_brt") }}
WHERE
  data_transacao >= "2023-08-01"
GROUP BY
  1,
  2
ORDER BY
  1,
  2