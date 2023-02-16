{{ 
config(
    alias='sumario_periodo'
)
}}

WITH
  viagem AS (
  SELECT
    data,
    trip_id,
    id_classificacao,
    COUNT(id_viagem) AS viagens_realizadas
  FROM
    {{ ref("viagem_completa_subsidio") }}
  WHERE
    -- TODO: def criterio data
    data BETWEEN "2023-01-16" AND "2023-01-31"
  GROUP BY
    1,
    2,
    3 ),
  -- 2. Junta informações de viagens planejadas às realizadas
  planejado AS (
  SELECT
    DISTINCT p.*,
    IFNULL(v.viagens_realizadas, 0) AS viagens_realizadas,
    IFNULL(v.viagens_realizadas, 0) AS viagens_subsidio,
    id_classificacao,
  FROM (
    SELECT
      consorcio,
      data,
      tipo_dia,
      trip_id_planejado AS trip_id,
      servico,
      vista,
      sentido,
      inicio_periodo,
      fim_periodo,
      CASE
        WHEN sentido = "C" THEN MAX(distancia_planejada)
      ELSE
      SUM(distancia_planejada)
    END
      AS distancia_planejada,
      MAX(distancia_total_planejada) AS distancia_total_planejada,
      NULL AS viagens_planejadas
    FROM
      {{ ref("viagem_planejada") }}
    WHERE
      -- TODO: def criterio data
      data BETWEEN "2023-01-16" AND "2023-01-31"
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9 ) AS p
  LEFT JOIN
    viagem v
  ON
    v.trip_id = p.trip_id
    AND v.data = p.data )
  -- 3. Adiciona informações de distância total
SELECT
  * EXCEPT(distancia_planejada,
    distancia_total_planejada),
  distancia_total_planejada,
  ROUND(viagens_subsidio * distancia_planejada, 3) AS distancia_total_subsidio,
  ROUND(viagens_realizadas * distancia_planejada, 3) AS distancia_total_aferida,
  '' AS versao_modelo
FROM
  planejado