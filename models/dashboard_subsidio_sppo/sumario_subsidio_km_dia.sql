{{ 
config(
    alias='sumario_km_dia'
)
}}

-- Calcula percentual de km executado de todos os serviços para cada dia, 
-- desde o início do subsídio (jun/22) até a data máxima da última quinzena apurada.

-- 1. Soma viagens realizadas de diferentes sentidos do mesmo serviço por id_classificacao
-- A km planejada é por serviço e nao sentido, portanto a distancia_total_planejada 
-- da sumario_periodo já é a total do serviço para ambos os sentidos e por isso nao somamos.
WITH
  sumario AS (
  SELECT
    consorcio,
    data,
    tipo_dia,
    servico,
    id_classificacao,
    ROUND(SUM(viagens_planejadas), 3) AS viagens_planejadas,
    ROUND(SUM(viagens_subsidio), 3) AS viagens_subsidio,
    MAX(distancia_total_planejada) AS distancia_total_planejada,
    ROUND(SUM(distancia_total_subsidio), 3) AS distancia_total_subsidio,
  FROM
    {{ ref("sumario_subsidio_dia_periodo") }}
  GROUP BY
    1,
    2,
    3,
    4,
    5 )
-- 2. Recupera o valor do km para cada data planejada
SELECT
  s.*,
  v.valor_subsidio_por_km,
  ROUND(distancia_total_subsidio * v.valor_subsidio_por_km, 2) AS valor_total_aferido
FROM
  sumario s
LEFT JOIN (
  SELECT
    *
  FROM
    {{ ref("subsidio_valor_subsidio_dia") }}
  WHERE
    -- TODO: def criterio data
    data BETWEEN "2023-01-16" AND "2023-01-31") AS v
ON
  v.data = s.data AND
  v.id_classificacao = s.id_classificacao