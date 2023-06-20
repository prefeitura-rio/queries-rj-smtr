/*  Query para criar a view com dados de 2022*/ 
CREATE OR REPLACE VIEW `rj-smtr-dev.dashboard_cimu.sumario_servico_dia_2022` AS

SELECT
  s.`data`,
  s.tipo_dia,
  s.servico,
  CAST(s.viagens AS INT) AS viagens,
  ROUND(s.km_apurada, 2) AS km_apurada,
  ROUND(km_planejada, 2) AS km_planejada,
  ROUND(perc_km_planejada, 2) AS perc_km_planejada,
  ROUND(valor_subsidio_pago, 2) AS valor_subsidio_pago,
  t.consorcio  
FROM
  `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico` s
LEFT JOIN (
  SELECT DISTINCT
    data,
    servico,
    consorcio,
    km_apurada
  FROM
    `rj-smtr.dashboard_subsidio_sppo.sumario_servico_tipo_viagem_dia`
) t ON s.servico = t.servico AND s.`data` = t.`data`
WHERE
  s.`data` < '2023-01-16' 
GROUP BY
  s.`data`,
  s.tipo_dia,
  s.servico,
  viagens,
  km_apurada,
  km_planejada,
  perc_km_planejada,
  valor_subsidio_pago,
  t.consorcio
ORDER BY
  s.servico,
  s.`data`;