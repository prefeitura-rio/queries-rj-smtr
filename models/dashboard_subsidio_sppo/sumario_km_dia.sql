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
    {{ ref("sumario_periodo") }}
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
    {{ ref("valor_subsidio_dia") }}
  WHERE
    -- TODO: def criterio data
    data BETWEEN "2023-01-16" AND "2023-01-31") AS v
ON
  v.data = s.data AND
  v.id_classificacao = s.id_classificacao