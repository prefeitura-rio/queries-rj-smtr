SELECT
  EXTRACT(YEAR
  FROM
    DATA) AS ano,
  consorcio,
  ROUND(SUM(km)/POW(10,6), 2) km_milhoes,
  ROUND(SUM(remuneracao_tarifaria)/POW(10,6), 2) remuneracao_tarifaria_milhoes,
  ROUND(SUM(subsidio)/POW(10,6), 2) subsidio_milhoes,
  ROUND(SUM(receita_aferida)/POW(10,6), 2) AS receita_aferida_milhoes,
  ROUND(SUM(receita_esperada)/POW(10,6), 2) AS receita_esperada_milhoes,
  ROUND(SUM(desconto)/POW(10,6), 2) AS desconto_milhoes,
  ROUND(SUM(saldo)/POW(10, 6), 2) AS saldo_milhoes
FROM
  {{ ref("encontro_contas_consorcio_dia_cenario_2") }}
GROUP BY
  1,
  2