{{
  config(
    materialized="view"
  )
}}

WITH
  viagem_planejada_hora AS (
  SELECT
    data,
    EXTRACT(Hour
    FROM
      inicio_periodo) AS hora,
  FROM
    {{ref('viagem_planejada')}}
),
  viagem_completa_hora AS (
  SELECT
    data,
    EXTRACT(Hour
    FROM
      inicio_periodo) AS hora,
  FROM
    {{ref('viagem_completa')}}
)
SELECT
  *,
  COUNT(vp.hora) AS viagens_planejadas,
  COUNT(vc.hora) AS viagens_completas
FROM
  viagem_planejada_hora vp
FULL OUTER JOIN
  viagem_completa_hora vc
USING
  (data,
    hora)
GROUP BY
  data,
  hora
order by data, hora