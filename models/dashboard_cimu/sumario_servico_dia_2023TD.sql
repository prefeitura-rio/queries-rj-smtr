/* Query para criar a view com dados de 2023 em diante (To Date) */

SELECT
  s.`data`,
  s.tipo_dia,
  s.servico,
  CAST(s.viagens AS INT) AS viagens,
  ROUND(s.km_apurada, 2) AS km_apurada,
  ROUND(km_planejada, 2) AS km_planejada,
  ROUND(perc_km_planejada, 2) AS perc_km_planejada,
  ROUND(valor_subsidio_pago, 2) AS valor_subsidio_pago,
  ROUND(valor_penalidade, 2) AS valor_penalidade,
  ROUND(valor_subsidio_pago + COALESCE(valor_penalidade, 0), 2) AS valor_final_subsidio,
  t.consorcio,
  COALESCE(MAX(CASE WHEN t.tipo_viagem = 'Licenciado com ar e autuado (023.II)' THEN t.km_apurada END), 0) AS km_apurada_licenciado_ar_autuado, /* ar inoperante*/
  COALESCE(MAX(CASE WHEN t.tipo_viagem = 'Nao licenciado' THEN t.km_apurada END), 0) AS km_apurada_n_licenciado, /* É a coluna "km não identificado" no dash */
  COALESCE(MAX(CASE WHEN t.tipo_viagem = 'Licenciado sem ar' THEN t.km_apurada END), 0) AS km_apurada_licenciado_sem_ar, /*sem ar*/
  COALESCE(MAX(CASE WHEN t.tipo_viagem = 'Licenciado com ar e não autuado (023.II)' THEN t.km_apurada END), 0) AS km_apurada_licenciado_ar_n_autuado /*com ar*/
FROM
  `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico` s
LEFT JOIN (
  SELECT DISTINCT
    data,
    servico,
    consorcio,
    tipo_viagem,
    km_apurada
  FROM
    `rj-smtr.dashboard_subsidio_sppo.sumario_servico_tipo_viagem_dia`
) t ON s.servico = t.servico AND s.`data` = t.`data`
WHERE
  s.`data` >= '2023-01-16' 
  AND s.`data` NOT BETWEEN '2023-02-17' AND '2023-02-22' /* remover intervalo do carnaval*/
GROUP BY
  s.`data`,
  s.tipo_dia,
  s.servico,
  viagens,
  km_apurada,
  km_planejada,
  perc_km_planejada,
  valor_subsidio_pago,
  valor_penalidade,
  valor_final_subsidio,
  t.consorcio