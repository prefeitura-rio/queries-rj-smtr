/* Remuneração aferida */

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
    FROM {{ source('dashboard_subsidio_sppo', 'sumario_servico_dia_tipo') }}  
    WHERE
    data BETWEEN DATE('{{ var('data_inicio_quinzena') }}') AND DATE('{{ var('data_fim_quinzena') }}')

)
SELECT * FROM tabela_base
