/* Aba 1 - Tabela 4 - Dados Dia consórcio */
 
WITH 
  dados_quilometragem AS (
    SELECT consorcio,
           FORMAT_TIMESTAMP('%d-%m-%Y', TIMESTAMP(data)) AS data,
           ROUND(SUM(valor_total_subsidio), 2) AS valor_a_pagar,
           ROUND(SUM(distancia_total_subsidio), 2) AS km_validados
    FROM {{ ref('sumario_servico_dia_tipo_15d') }}
    GROUP BY consorcio, data 
    ORDER BY consorcio, data
  ) 
  SELECT * FROM dados_quilometragem
