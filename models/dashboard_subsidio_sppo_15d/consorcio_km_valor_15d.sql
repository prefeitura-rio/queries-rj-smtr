/* Aba 1 - Tabela - 1 - Quilometragem e valor por consórcios*/

WITH 
  dados_quilometragem AS (
    SELECT consorcio, 
           ROUND(SUM(distancia_total_planejada), 2) AS km_planejada,
           ROUND(SUM(distancia_total_subsidio), 2) AS km_validada,
           ROUND(SUM(CASE WHEN valor_total_subsidio > 0 THEN distancia_total_subsidio ELSE 0 END), 2) AS km_a_ser_pago,
           ROUND(SUM(valor_total_subsidio), 2) AS valor_a_ser_pago
    FROM {{ ref('sumario_servico_dia_tipo_15d') }} 
    GROUP BY consorcio
  ) 
SELECT 
  consorcio,
  km_planejada,
  km_validada,
  ROUND(km_validada / km_planejada, 2) AS percentual_plan_valid,
  km_a_ser_pago,
  ROUND(km_a_ser_pago / km_planejada, 2) AS percentual_plan_pago,
  valor_a_ser_pago
FROM dados_quilometragem

UNION ALL

SELECT 
  'Total' AS consorcio,
  ROUND(SUM(km_planejada), 2) AS km_planejada,
  ROUND(SUM(km_validada), 2) AS km_validada,
  ROUND(SUM(km_validada) / SUM(km_planejada), 2) AS percentual_plan_valid,
  ROUND(SUM(km_a_ser_pago), 2) AS km_a_ser_pago,
  ROUND(SUM(km_a_ser_pago) / SUM(km_planejada), 2) AS percentual_plan_pago,
  ROUND(SUM(valor_a_ser_pago), 2) AS valor_a_ser_pago
FROM dados_quilometragem
