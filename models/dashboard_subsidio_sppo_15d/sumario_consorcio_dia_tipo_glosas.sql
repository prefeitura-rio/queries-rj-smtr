-- valor por consórcio
-- falta validar

SELECT 
  DATA,
  consorcio,
  ROUND(SUM(viagens),0) AS viagens,
  ROUND(SUM(km_planejada),2) AS km_planejada,
  ROUND((SUM(km_apurada)/SUM(km_planejada)* 100), 2) AS perc_km_planejada,
  ROUND(SUM(km_apurada),2) AS km_apurada,
  ROUND(SUM(km_apurada_tipo_viagem),2) AS km_apurada_tipo_viagem,
  ROUND(SUM(km_paga),2) AS km_paga,
  ROUND(SUM(km_paga_excl_autuacoes),2) AS km_paga_excl_autuacoes,
  ROUND(SUM(valor_planejado),2) AS valor_planejado,
  ROUND(SUM(valor_apurado),2) AS valor_apurado,
  ROUND(SUM(valor_apurado_tipo_viagem),2) AS valor_apurado_tipo_viagem,
  ROUND(SUM(valor_pago_sem_autacoes),2) AS valor_pago_sem_autacoes,
  ROUND(SUM(valor_pago_com_autacoes),2) AS valor_pago_com_autacoes,
  ROUND(SUM(valor_subsidio_com_glosa),2) AS valor_subsidio_com_glosa,
  ROUND(SUM(valor_subsidio_tipo_viagem),2) AS valor_subsidio_tipo_viagem,
  ROUND(SUM(glosa_penalidade),2) AS glosa_penalidade,
  ROUND(SUM(glosa_subsidio_tipo_viagem),2) AS glosa_subsidio_tipo_viagem,
  ROUND(SUM(km_sem_viagem_apurada),2) AS km_sem_viagem_apurada,
  ROUND(SUM(km_com_ar_nao_autuado),2) AS km_com_ar_nao_autuado,
  ROUND(SUM(km_sem_ar_nao_autuado),2) AS km_sem_ar_nao_autuado,
  ROUND(SUM(km_autuado_por_ar_inoperante),2) AS km_autuado_por_ar_inoperante,
  ROUND(SUM(km_autuado_por_seguranca),2) AS km_autuado_por_seguranca,
  ROUND(SUM(subsidio_sem_viagem_apurada),2) AS subsidio_sem_viagem_apurada,
  ROUND(SUM(subsidio_com_ar_nao_autuado),2) AS subsidio_com_ar_nao_autuado,
  ROUND(SUM(subsidio_sem_ar_nao_autuado),2) AS subsidio_sem_ar_nao_autuado,
  ROUND(SUM(subsidio_autuado_por_ar_inoperante),2) AS subsidio_autuado_por_ar_inoperante,
  ROUND(SUM(subsidio_autuado_por_seguranca),2) AS subsidio_autuado_por_seguranca,
  ROUND(SUM(glosa_sem_viagem_apurada),2) AS glosa_sem_viagem_apurada,
  ROUND(SUM(glosa_com_ar_nao_autuado),2) AS glosa_com_ar_nao_autuado,
  ROUND(SUM(glosa_sem_ar_nao_autuado),2) AS glosa_sem_ar_nao_autuado,
  ROUND(SUM(glosa_autuado_por_ar_inoperante),2) AS glosa_autuado_por_ar_inoperante,
  ROUND(SUM(glosa_autuado_por_seguranca),2) AS glosa_autuado_por_seguranca

 FROM `rj-smtr-dev.dashboard_subsidio_sppo_15d.sumario_servico_dia_tipo_glosas` 

 WHERE
      DATA BETWEEN DATE('{{ var('data_inicio_quinzena') }}') AND DATE('{{ var('data_fim_quinzena') }}')

GROUP BY 1,2
