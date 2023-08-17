-- TABELA TEMPORÁRIA até decisão judicial final 
-- TODO: parametrizar valores do subsidio por KM de acordo com a tabela de parametros, verificar dados pré-novas regras (4/07)
SELECT
  consorcio,
  data,
  tipo_dia,
  servico,
  viagens AS viagens_subsidio,
  km_planejada AS distancia_total_planejada,
  km_apurada AS distancia_total_subsidio,
  0 AS valor_total_aferido,
  perc_km_planejada AS perc_distancia_total_subsidio,
  -- Valor total sem glosas: quando existe subsidio (POD>80%), adiciona o valor glosado por tipo de viagem ao total
  CASE
    WHEN perc_km_planejada >= 80 THEN ROUND(valor_subsidio_pago + COALESCE(km_apurada_autuado_ar_inoperante * 2.81, 0) + COALESCE(km_apurada_autuado_seguranca * 2.81, 0) + COALESCE(km_apurada_autuado_limpezaequipamento * 2.81, 0) + COALESCE(km_apurada_licenciado_sem_ar_n_autuado * 0.84, 0), 2)
  ELSE
  0
END
  AS valor_total_subsidio,
  viagens_n_licenciado,
  COALESCE(km_apurada_n_licenciado, 0) AS km_apurada_n_licenciado,
  viagens_autuado_ar_inoperante,
  COALESCE(km_apurada_autuado_ar_inoperante, 0) AS km_apurada_autuado_ar_inoperante,
  viagens_autuado_seguranca,
  COALESCE(km_apurada_autuado_seguranca, 0) AS km_apurada_autuado_seguranca,
  viagens_autuado_limpezaequipamento,
  COALESCE(km_apurada_autuado_limpezaequipamento, 0) AS km_apurada_autuado_limpezaequipamento,
  viagens_licenciado_sem_ar_n_autuado,
  COALESCE(km_apurada_licenciado_sem_ar_n_autuado, 0) AS km_apurada_licenciado_sem_ar_n_autuado,
  viagens_licenciado_com_ar_n_autuado,
  COALESCE(km_apurada_licenciado_com_ar_n_autuado, 0) AS km_apurada_licenciado_com_ar_n_autuado
FROM
  rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_tipo