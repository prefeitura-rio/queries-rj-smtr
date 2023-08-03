-- Aba 1 - Tabela 3 - Resumo por consórcio 

    SELECT consorcio, 
           ROUND(SUM(valor_total_subsidio), 2) AS valor_a_pagar,
           ROUND(SUM(viagens_subsidio), 0) AS meias_viagens,
           ROUND(SUM(distancia_total_planejada), 2) AS km_planejados,
           ROUND(SUM(distancia_total_subsidio), 2) AS km_validados,
           ROUND(SUM(CASE WHEN valor_total_subsidio > 0 THEN distancia_total_subsidio ELSE 0 END), 2) AS km_pagos, /* é a soma das km apuradas apenas para as linhas com valor_total_subsidio > 0 */
           ROUND(SUM(IFNULL(km_apurada_autuado_ar_inoperante, 0)), 2) AS km_multado_ar,
           ROUND(SUM(IFNULL(km_apurada_autuado_seguranca, 0)), 2) AS km_multado_seguranca,
           ROUND(SUM(IFNULL(km_apurada_autuado_limpezaequipamento, 0)), 2) AS km_multado_limpeza_equip, 
           ROUND(SUM(IFNULL(km_apurada_licenciado_sem_ar_n_autuado, 0)), 2) AS km_sem_ar,
           ROUND(SUM(IFNULL(km_apurada_licenciado_com_ar_n_autuado, 0)), 2) AS km_com_ar,
                      ROUND(SUM(IFNULL(km_apurada_n_licenciado, 0)), 2) AS km_sem_identificar
    FROM {{ ref('sumario_servico_dia_tipo_15d') }} 
    GROUP BY consorcio
 