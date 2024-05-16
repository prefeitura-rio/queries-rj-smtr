WITH
  q1 AS (
  SELECT
    FORMAT_DATE('%Y-%m-Q1', date) AS quinzena,
    date AS data_inicial_quinzena,
    DATE_ADD(date, INTERVAL 14 DAY) AS data_final_quinzena
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2022-06-01', '2023-12-31', INTERVAL 1 MONTH)) AS date ),
  q2 AS (
  SELECT
    FORMAT_DATE('%Y-%m-Q2', date) AS quinzena,
    DATE_ADD(date, INTERVAL 15 DAY) AS data_inicial_quinzena,
    LAST_DAY(date) AS data_final_quinzena
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2022-06-01', '2023-12-31', INTERVAL 1 MONTH)) AS date ),
  quinzenas AS (
  SELECT
    *
  FROM
    q1
  UNION ALL
  SELECT
    *
  FROM
    q2
  ORDER BY
    data_inicial_quinzena )
SELECT
  quinzena,
  data_inicial_quinzena,
  data_final_quinzena,
  consorcio,
  servico,
  COUNT(DATA) AS quantidade_dias_subsidiado,
  SUM(km_subsidiada) AS km_subsidiada,
  SUM(receita_total_esperada) AS receita_total_esperada,
  SUM(receita_tarifaria_esperada) AS receita_tarifaria_esperada,
  SUM(subsidio_esperado) AS subsidio_esperado,
  SUM(subsidio_glosado) AS subsidio_glosado,
  SUM(receita_total_aferida) AS receita_total_aferida,
  SUM(receita_tarifaria_aferida) AS receita_tarifaria_aferida,
  SUM(subsidio_pago) AS subsidio_pago,
  SUM(saldo) AS saldo
FROM
  quinzenas qz
LEFT JOIN
  {{ ref("balanco_servico_dia") }} bs
ON
  bs.data BETWEEN qz.data_inicial_quinzena
  AND qz.data_final_quinzena
GROUP BY
  quinzena,
  data_inicial_quinzena,
  data_final_quinzena,
  consorcio,
  servico
ORDER BY
  data_inicial_quinzena,
  consorcio,
  servico