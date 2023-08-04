/* Remuneração aferida */

-- parametrizar a query
-- validar dados com a planilha no excel para 1q julho e 2q junho
-- dados a partir de 16/01/2023

WITH
tabela_base AS (
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
    valor_subsidio_pago + valor_penalidade AS valor_total_subsidio,
    COALESCE(viagens_n_licenciado, 0) AS viagens_n_licenciado,
    COALESCE(km_apurada_n_licenciado, 0) AS km_apurada_n_licenciado,
    COALESCE(viagens_autuado_ar_inoperante,0) AS viagens_autuado_ar_inoperante,
    COALESCE(km_apurada_autuado_ar_inoperante, 0) AS km_apurada_autuado_ar_inoperante,
    COALESCE(viagens_autuado_seguranca,0) AS viagens_autuado_seguranca,
    COALESCE(km_apurada_autuado_seguranca, 0) AS km_apurada_autuado_seguranca,
    COALESCE(viagens_autuado_limpezaequipamento,0) AS viagens_autuado_limpezaequipamento,
    COALESCE(km_apurada_autuado_limpezaequipamento, 0) AS km_apurada_autuado_limpezaequipamento,
    COALESCE(viagens_licenciado_sem_ar_n_autuado,0) AS viagens_licenciado_sem_ar_n_autuado,
    COALESCE(km_apurada_licenciado_sem_ar_n_autuado, 0) AS km_apurada_licenciado_sem_ar_n_autuado,
    COALESCE(viagens_licenciado_com_ar_n_autuado,0) AS viagens_licenciado_com_ar_n_autuado,
    COALESCE(km_apurada_licenciado_com_ar_n_autuado, 0) AS km_apurada_licenciado_com_ar_n_autuado
    FROM `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_tipo`  
),

 valores_glosados AS (
  SELECT *,
    ROUND(distancia_total_planejada * 2.81, 2) AS valor_planejado,
    ROUND(distancia_total_subsidio * 2.81, 2) AS valor_apurado,
    ROUND(valor_total_subsidio, 2) AS valor_a_pagar,
    CASE -- não cumprimento de 80%
       WHEN perc_distancia_total_subsidio < 80 
       THEN ROUND((km_apurada_n_licenciado + km_apurada_autuado_ar_inoperante + 
       km_apurada_autuado_seguranca + km_apurada_autuado_limpezaequipamento +
       km_apurada_licenciado_sem_ar_n_autuado + km_apurada_licenciado_com_ar_n_autuado) * 2.81, 2)
       ELSE 0 
       END AS n_cumpr_80_porcento,
    CASE -- multas abaixo de 60%
       WHEN valor_total_subsidio < 0 
       THEN ROUND(-valor_total_subsidio, 2)
       ELSE 0 
       END AS multas_abaixo_60,
    CASE -- sem licenciamento
       WHEN perc_distancia_total_subsidio > 80 
       THEN ROUND((km_apurada_n_licenciado * 2.81), 2)
       ELSE 0 
       END AS reducao_sem_licenciamento,
    CASE -- sem ar
       WHEN perc_distancia_total_subsidio > 80 
       THEN ROUND(km_apurada_licenciado_sem_ar_n_autuado * (2.81 - 1.97), 2)
       ELSE 0 
       END AS reducao_sem_ar,
    CASE -- ar inoperante
       WHEN perc_distancia_total_subsidio > 80 
       THEN ROUND((km_apurada_autuado_ar_inoperante * 2.81), 2)
       ELSE 0 
       END AS reducao_ar_inoperante,
   CASE -- limpeza e equipamento
       WHEN perc_distancia_total_subsidio > 80 
       THEN ROUND((km_apurada_autuado_limpezaequipamento * 2.81), 2)
       ELSE 0 
       END AS reducao_limpezaequipamento,
   CASE -- segurança
       WHEN perc_distancia_total_subsidio > 80 
       THEN ROUND((km_apurada_autuado_seguranca * 2.81), 2)
       ELSE 0 
       END AS reducao_seguranca

  FROM tabela_base 
)
SELECT consorcio, 
       data, 
       tipo_dia, 
       servico, 
       valor_planejado,
       valor_apurado,
       valor_a_pagar,
       n_cumpr_80_porcento,
       multas_abaixo_60,
       reducao_sem_licenciamento,
       reducao_sem_ar,
       reducao_ar_inoperante,
       reducao_limpezaequipamento,
       reducao_seguranca

FROM valores_glosados





