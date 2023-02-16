{{ 
config(
    alias='sumario_dia'
)
}}

WITH
  sumario AS (
  SELECT
    consorcio,
    data,
    tipo_dia,
    servico,
    ROUND(SUM(viagens_planejadas), 3) AS viagens_planejadas,
    ROUND(SUM(viagens_subsidio), 3) AS viagens_subsidio,
    MAX(distancia_total_planejada) AS distancia_total_planejada,
    -- Distância percorrida por veículo não licenciado não é considerada
    ROUND(SUM (CASE
      WHEN id_classificacao != 1 THEN distancia_total_subsidio
      ELSE 0
    END), 3) AS distancia_total_subsidio,
    ROUND(SUM(valor_total_aferido), 2) AS valor_total_aferido
  FROM
    {{ ref("sumario_km_dia") }}
  GROUP BY
    1,
    2,
    3,
    4 ),
  -- 2. Calcula o % de cumprimento do km planejado
  perc_subsidio AS (
  SELECT
    *,
  IF
    (distancia_total_planejada = 0, NULL, ROUND(100*distancia_total_subsidio/distancia_total_planejada, 2)) AS perc_distancia_total_subsidio
  FROM
    sumario )
-- 3. Define o valor total a ser pago com base no cumprimento do km planejado - mínimo de 80%
-- Abaixo de 60% ainda há aplicação de dedução 
SELECT
  p.*,
  CASE
    WHEN ((perc_distancia_total_subsidio IS NULL) OR (perc_distancia_total_subsidio >= 60 AND perc_distancia_total_subsidio < 80)) THEN 0
    WHEN perc_distancia_total_subsidio < 60 THEN -v.valor_multa
  ELSE
  valor_total_aferido
END
  AS valor_total_subsidio
FROM
  perc_subsidio AS p
LEFT JOIN
  {{ ref("valor_multa_dia") }} AS v
ON
  v.data = p.data AND
  p.perc_distancia_total_subsidio >= v.faixa_km.start AND
  p.perc_distancia_total_subsidio < v.faixa_km.finish
ORDER BY
  data