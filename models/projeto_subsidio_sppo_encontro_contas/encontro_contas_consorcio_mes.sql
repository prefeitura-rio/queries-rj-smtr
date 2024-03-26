SELECT
  EXTRACT(YEAR
  FROM
    DATA) AS ano,
  EXTRACT(MONTH
  FROM
    DATA) AS mes,
  consorcio,
  ROUND(SUM(km_apurada)/POW(10,6), 2) km_apurada_milhoes,
  ROUND(SUM(km_planejada)/POW(10,6), 2) km_planejada_milhoes,
  ROUND(SUM(km_subsidiada)/POW(10,6), 2) km_subsidiada_milhoes,
  ROUND(SUM(remuneracao_tarifaria)/POW(10,6), 2) remuneracao_tarifaria_milhoes,
  ROUND(SUM(subsidio)/POW(10,6), 2) subsidio_milhoes,
  ROUND(SUM(receita_aferida)/POW(10,6), 2) AS receita_aferida_milhoes,
  ROUND(SUM(receita_esperada)/POW(10,6), 2) AS receita_esperada_milhoes,
  ROUND(SUM(desconto)/POW(10,6), 2) AS desconto_milhoes,
  ROUND(SUM(saldo)/POW(10, 6), 2) AS saldo_milhoes,
  ROUND(SUM(quantidade_passageiros_total)/POW(10, 6), 2) AS quantidade_passageiros_total_milhoes,
FROM
  {{ ref("encontro_contas_consorcio_dia") }}
GROUP BY
  1,
  2,
  3