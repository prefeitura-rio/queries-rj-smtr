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
    ROUND(SUM(distancia_total_subsidio), 3) AS distancia_total_subsidio,
  FROM
    {{ ref("sumario_subsidio_dia_periodo") }}
  GROUP BY
    1,
    2,
    3,
    4 ),
  valor AS (
  SELECT
    s.*,
    v.valor_subsidio_por_km,
    ROUND(distancia_total_subsidio * v.valor_subsidio_por_km, 2) AS valor_total_aferido,
    ROUND(100*distancia_total_subsidio/distancia_total_planejada, 2) AS perc_distancia_total_subsidio
  FROM
    sumario s
  LEFT JOIN (
    SELECT
      *
    FROM
      {{ ref("subsidio_data_versao_efetiva") }}
    WHERE
      data BETWEEN "2022-06-01" AND DATE("{{ var("end_date") }}")) AS v
  ON
    v.data = s.data )
SELECT
  *,
  CASE
    WHEN perc_distancia_total_subsidio < {{ var("perc_distancia_total_subsidio") }} THEN 0
  ELSE
  valor_total_aferido
END
  AS valor_total_subsidio
FROM
  valor