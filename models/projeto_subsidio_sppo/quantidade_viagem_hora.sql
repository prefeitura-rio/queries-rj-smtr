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
    servico
  FROM
    {{ref('viagem_planejada')}}
),
  viagem_completa_hora AS (
  SELECT
    data,
    EXTRACT(Hour
    FROM
      inicio_periodo) AS hora,
    servico_realizado AS servico
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
    hora,servico)
GROUP BY
  data,
  hora, servico
order by data, hora