WITH

SELECT
  s.`data`,
  tipo_dia,
  s.servico,
  CAST(viagens AS INT) AS viagens,
  ROUND(km_apurada, 2) AS km_apurada,
  ROUND(km_planejada, 2) AS km_planejada,
  ROUND(perc_km_planejada, 2) AS perc_km_planejada,
  ROUND(valor_subsidio_pago, 2) AS valor_subsidio_pago,
  ROUND(valor_penalidade, 2) AS valor_penalidade,
  ROUND(valor_subsidio_pago + COALESCE(valor_penalidade, 0), 2) AS valor_final_subsidio 

FROM
  `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico` s

WHERE
  s.`data` NOT BETWEEN '2023-02-17' AND '2023-02-22'; /* Excluir intervalo do carnaval */
