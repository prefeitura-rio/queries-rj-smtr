SELECT
  EXTRACT(YEAR
  FROM
    DATA) AS ano,
  EXTRACT(MONTH
  FROM
    DATA) AS mes,
  consorcio,
  ROUND(SUM(km_apurada), 2) km_apurada,
  ROUND(SUM(km_planejada), 2) km_planejada,
  ROUND(SUM(km_subsidiada), 2) km_subsidiada,
  ROUND(SUM(remuneracao_tarifaria), 2) remuneracao_tarifaria,
  ROUND(SUM(subsidio), 2) subsidio,
  ROUND(SUM(receita_aferida), 2) AS receita_aferida,
  ROUND(SUM(receita_esperada), 2) AS receita_esperada,
  ROUND(SUM(desconto), 2) AS desconto,
  ROUND(SUM(saldo), 2) AS saldo,
  ROUND(SUM(quantidade_passageiros_total), 2) AS quantidade_passageiros_total,
FROM
  {{ ref("encontro_contas_consorcio_dia") }}
GROUP BY
  1,
  2,
  3