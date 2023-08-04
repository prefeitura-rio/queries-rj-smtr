/* Punições por consórcio - Aba 2 - tabela 1*/

WITH results AS (
  SELECT consorcio,
    ROUND(SUM(distancia_total_planejada * {{ var('valor_subsidio_com_ar') }}), 2) AS valor_planejado,
    ROUND(SUM(distancia_total_subsidio * {{ var('valor_subsidio_com_ar') }}), 2) AS valor_apurado,
    ROUND(SUM(valor_total_subsidio), 2) AS valor_a_pagar,
    SUM(CASE -- não cumprimento de 80%
       WHEN perc_distancia_total_subsidio < 80 
       THEN ROUND((km_apurada_n_licenciado + km_apurada_autuado_ar_inoperante + 
       km_apurada_autuado_seguranca + km_apurada_autuado_limpezaequipamento +
       km_apurada_licenciado_sem_ar_n_autuado + km_apurada_licenciado_com_ar_n_autuado) * {{ var('valor_subsidio_com_ar') }}, 2)
       ELSE 0 
       END) AS n_cumpr_80_porcento,
    SUM(CASE -- não cumprimento de 60%
       WHEN valor_total_subsidio < 0 
       THEN ROUND(-valor_total_subsidio, 2)
       ELSE 0 
       END) AS multas_abaixo_60,
    SUM(CASE -- sem licenciamento
       WHEN perc_distancia_total_subsidio > 80 
       THEN ROUND((km_apurada_n_licenciado * {{ var('valor_subsidio_com_ar') }}), 2)
       ELSE 0 
       END) AS reducao_sem_licenciamento,
    --  reducao_sem_ar com diferença pequena da planilha, com exceção da Transcarioca que bate (será que os dados foram atualizados?)
    SUM(CASE -- sem ar
       WHEN perc_distancia_total_subsidio > 80 
       THEN ROUND(km_apurada_licenciado_sem_ar_n_autuado * ({{ var('valor_subsidio_com_ar') }} - {{ var('valor_subsidio_sem_ar') }}), 2)
       ELSE 0 
       END) AS reducao_sem_ar,
    SUM(CASE -- ar inoperante
       WHEN perc_distancia_total_subsidio > 80 
       THEN ROUND((km_apurada_autuado_ar_inoperante * {{ var('valor_subsidio_com_ar') }}), 2)
       ELSE 0 
       END) AS reducao_ar_inoperante,
   SUM(CASE -- limpeza e equipamento
       WHEN perc_distancia_total_subsidio > 80 
       THEN ROUND((km_apurada_autuado_limpezaequipamento * {{ var('valor_subsidio_com_ar') }}), 2)
       ELSE 0 
       END) AS reducao_limpezaequipamento,
   SUM(CASE -- segurança
       WHEN perc_distancia_total_subsidio > 80 
       THEN ROUND((km_apurada_autuado_seguranca * {{ var('valor_subsidio_com_ar') }}), 2)
       ELSE 0 
       END) AS reducao_seguranca

  FROM {{ ref('sumario_servico_dia_tipo_15d') }} 
  GROUP BY consorcio
)
SELECT * FROM results
UNION ALL
SELECT 'Total' AS consorcio, SUM(valor_planejado), SUM(valor_apurado), SUM(valor_a_pagar), SUM(n_cumpr_80_porcento), 
       SUM(multas_abaixo_60), SUM(reducao_sem_licenciamento), SUM(reducao_sem_ar), SUM(reducao_ar_inoperante),
       SUM(reducao_limpezaequipamento), SUM(reducao_seguranca)
FROM results
