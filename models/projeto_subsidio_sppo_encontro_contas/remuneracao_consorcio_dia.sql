{{ 
config(
    materialized="view",
)
}}

SELECT
  ano,
  DATA,
  consorcio,
  irk,
  irk_tarifa_publica,
  SUM(viagens) AS viagens,
  SUM(quilometragem) AS quilometragem,
  SUM(desconto_subsidio) AS desconto_subsidio,
  SUM(receita_esperada) AS receita_esperada,
  SUM(subsidio_esperado) AS subsidio_esperado,
  SUM(receita_tarifaria_esperada) AS receita_tarifaria_esperada,
  SUM(receita_tarifaria) AS receita_tarifaria,
  SUM(subsidio) AS subsidio,
  SUM(receita_aferida) AS receita_aferida,
  SUM(diff_tarifario_esperado) AS diff_tarifario_esperado,
  SUM(diff_aferido_esperado) AS diff_aferido_esperado
FROM
  {{ ref("remuneracao_servico_dia") }} 
GROUP BY
  1,
  2,
  3,
  4,
  5