/* Aba 1 - Tabela - 2 - Percentual da km por tipo de viagem e consórcio */ 

WITH 
  categorias_km AS (
    SELECT consorcio, 
           ROUND(SUM(IFNULL(km_apurada_n_licenciado, 0)), 2) AS km_n_licenciado,
           ROUND(SUM(IFNULL(km_apurada_autuado_ar_inoperante, 0)), 2) AS km_multado_ar,
           ROUND(SUM(IFNULL(km_apurada_autuado_seguranca, 0)), 2) AS km_multado_seguranca,
           ROUND(SUM(IFNULL(km_apurada_autuado_limpezaequipamento, 0)), 2) AS km_multado_limpeza_equip, 
           ROUND(SUM(IFNULL(km_apurada_licenciado_sem_ar_n_autuado, 0)), 2) AS km_sem_ar,
           ROUND(SUM(IFNULL(km_apurada_licenciado_com_ar_n_autuado, 0)), 2) AS km_com_ar
    FROM {{ ref('sumario_servico_dia_tipo_15d') }} 
    GROUP BY consorcio
  ),
  categorias_com_total AS (
    SELECT *, (km_n_licenciado + km_multado_ar + km_multado_seguranca + km_multado_limpeza_equip + km_sem_ar + km_com_ar) AS total_km
  FROM categorias_km
  ),
  categorias_com_total_linha AS (
  SELECT * FROM categorias_com_total
  UNION ALL
  SELECT 
    'Total', 
    ROUND(SUM(km_n_licenciado), 2),
    ROUND(SUM(km_multado_ar), 2),
    ROUND(SUM(km_multado_seguranca), 2),
    ROUND(SUM(km_multado_limpeza_equip), 2),
    ROUND(SUM(km_sem_ar), 2),
    ROUND(SUM(km_com_ar), 2),
    ROUND(SUM(total_km), 2)
  FROM categorias_com_total
  ),
  categorias_perc AS (
    SELECT consorcio,
       ROUND(km_n_licenciado / total_km, 2) AS perc_km_n_licenciado,
       ROUND(km_multado_ar / total_km, 2) AS perc_km_multado_ar,
       ROUND(km_multado_seguranca / total_km, 2) AS perc_km_multado_seguranca,
       ROUND(km_multado_limpeza_equip / total_km, 2) AS perc_km_multado_limpeza_equip,
       ROUND(km_sem_ar / total_km, 2) AS perc_km_sem_ar,
       ROUND(km_com_ar / total_km, 2) AS perc_km_com_ar
    FROM categorias_com_total_linha
  )
SELECT * FROM categorias_perc
