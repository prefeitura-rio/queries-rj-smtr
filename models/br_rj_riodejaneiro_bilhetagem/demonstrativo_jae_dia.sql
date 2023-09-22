-- Dado teste para comparação de valores
WITH
  transacao_agg AS (
  SELECT
    20001 AS agency_id, -- TODO: puxar do GTFS
    data,
    COUNT(*) AS n_transacoes,
    MAX(valor_transacao) AS valor_bilhete,
    SUM(valor_transacao) AS valor_bruto
  FROM
    {{ ref("transacao") }}
  WHERE
    data >= "2023-08-01"
  GROUP BY
    1,
    2 )
SELECT
  *,
  0.04 * valor_bruto AS tarifa_cbd,
  valor_bruto - (0.04 * valor_bruto) AS valor_liquido_cct,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM data) IN (1, 7) THEN DATE_ADD(DATE_TRUNC(data, WEEK(MONDAY)), INTERVAL 7 DAY)
  ELSE
    DATE_ADD(data, INTERVAL 1 DAY)
  END 
  AS data_liquidacao -- TODO: adicionar feriados
FROM
  transacao_agg t
ORDER BY 
  data
