-- TABELA TEMPORÁRIA até decisão judicial final 
-- Valores válidos entre 16-01-2023 e 31-12-2023 (subsidio_km = 2.81)

-- TODO: Consertar parametrizacao para demais datas (separar parametros em subsidio x desconto)
WITH 
parametros AS (
  SELECT 
    MAX(IF(status = 'Licenciado sem ar e não autuado', subsidio_km, NULL)) AS subsidio_km_sem_ar_n_autuado,
    MAX(IF(status = 'Licenciado com ar e não autuado', subsidio_km, NULL)) AS subsidio_km_sem_glosa 
  FROM 
    {{ ref("subsidio_parametros") }}
  WHERE
    data_inicio >= '2023-07-04'
)
SELECT
  consorcio,
  data,
  tipo_dia,
  servico,
  viagens AS viagens_subsidio,
  km_planejada AS distancia_total_planejada,
  km_apurada AS distancia_total_subsidio,
  null AS valor_total_aferido, -- TODO: Excluir essa coluna? é utilizada?
  perc_km_planejada AS perc_distancia_total_subsidio,
  -- Valor total sem glosas: quando existe subsidio (POD>80%), adiciona o valor glosado por tipo de viagem ao total
  CASE
    WHEN perc_km_planejada >= 80 THEN ROUND(COALESCE(valor_subsidio_pago, 0) + COALESCE(km_apurada_autuado_ar_inoperante * subsidio_km_sem_glosa, 0) + COALESCE(km_apurada_autuado_seguranca * subsidio_km_sem_glosa, 0) + COALESCE(km_apurada_autuado_limpezaequipamento * subsidio_km_sem_glosa, 0) + COALESCE(km_apurada_licenciado_sem_ar_n_autuado * (subsidio_km_sem_glosa - subsidio_km_sem_ar_n_autuado), 0), 2)
  ELSE
  0
END
  AS valor_total_subsidio,
  COALESCE(viagens_n_licenciado, 0) AS viagens_n_licenciado,
  COALESCE(km_apurada_n_licenciado, 0) AS km_apurada_n_licenciado,
  COALESCE(viagens_autuado_ar_inoperante, 0) AS viagens_autuado_ar_inoperante,
  COALESCE(km_apurada_autuado_ar_inoperante, 0) AS km_apurada_autuado_ar_inoperante,
  COALESCE(viagens_autuado_seguranca, 0) AS viagens_autuado_seguranca,
  COALESCE(km_apurada_autuado_seguranca, 0) AS km_apurada_autuado_seguranca,
  COALESCE(viagens_autuado_limpezaequipamento, 0) AS viagens_autuado_limpezaequipamento,
  COALESCE(km_apurada_autuado_limpezaequipamento, 0) AS km_apurada_autuado_limpezaequipamento,
  COALESCE(viagens_licenciado_sem_ar_n_autuado, 0) AS viagens_licenciado_sem_ar_n_autuado,
  COALESCE(km_apurada_licenciado_sem_ar_n_autuado, 0) AS km_apurada_licenciado_sem_ar_n_autuado,
  COALESCE(viagens_licenciado_com_ar_n_autuado, 0) AS viagens_licenciado_com_ar_n_autuado,
  COALESCE(km_apurada_licenciado_com_ar_n_autuado, 0) AS km_apurada_licenciado_com_ar_n_autuado
FROM
  {{ ref("sumario_servico_dia_tipo") }}
CROSS JOIN
  parametros