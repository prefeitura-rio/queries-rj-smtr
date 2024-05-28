{{
  config(
    materialized="view"
  )
}}
WITH
  viagem AS (
  SELECT
    DISTINCT p.consorcio,
    p.servico,
    v.id_viagem,
    p.inicio_periodo,
  FROM (
    SELECT
      DISTINCT consorcio,
      vista,
      DATA,
      tipo_dia,
      trip_id_planejado AS trip_id,
      servico,
      inicio_periodo,
      fim_periodo,
      id_tipo_trajeto
    FROM
      {{ ref('viagem_planejada') }}
    WHERE
      DATA = "2024-05-26" ) p
  INNER JOIN (
    SELECT
      DISTINCT *
    FROM
      {{ ref('viagem_conformidade') }}
    WHERE
      DATA = "2024-05-26" ) v
  ON
    v.trip_id = p.trip_id
    AND v.data = p.data ),
  viagem_planejada_hora AS (
  SELECT
    servico AS trip_short_name,
    consorcio,
    inicio_periodo,
    COUNT(id_viagem) AS quantidade_viagens_planejadas,
    CASE
      WHEN EXTRACT(HOUR FROM inicio_periodo) BETWEEN 0 AND 3 THEN '00:00 - 03:59'
      WHEN EXTRACT(HOUR
    FROM
      inicio_periodo) BETWEEN 4
    AND 7 THEN '04:00 - 07:59'
      WHEN EXTRACT(HOUR FROM inicio_periodo) BETWEEN 8 AND 11 THEN '08:00 - 11:59'
      WHEN EXTRACT(HOUR
    FROM
      inicio_periodo) BETWEEN 12
    AND 15 THEN '12:00 - 15:59'
      WHEN EXTRACT(HOUR FROM inicio_periodo) BETWEEN 16 AND 19 THEN '16:00 - 19:59'
      WHEN EXTRACT(HOUR
    FROM
      inicio_periodo) BETWEEN 20
    AND 23 THEN '20:00 - 23:59'
  END
    AS periodo
  FROM
    -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
    viagem
  -- WHERE
  --   DATA = "2024-05-26"
  GROUP BY
    servico,
    consorcio,
    periodo,
    inicio_periodo
  ORDER BY
    servico,
    consorcio,
    periodo ),
  viagem_completa_hora AS (
  SELECT
    consorcio,
    servico_realizado AS trip_short_name,
    COUNT(id_viagem) AS quantidade_viagens_completas,
    CASE
      WHEN EXTRACT(HOUR FROM inicio_periodo) BETWEEN 0 AND 3 THEN '00:00 - 03:59'
      WHEN EXTRACT(HOUR
    FROM
      inicio_periodo) BETWEEN 4
    AND 7 THEN '04:00 - 07:59'
      WHEN EXTRACT(HOUR FROM inicio_periodo) BETWEEN 8 AND 11 THEN '08:00 - 11:59'
      WHEN EXTRACT(HOUR
    FROM
      inicio_periodo) BETWEEN 12
    AND 15 THEN '12:00 - 15:59'
      WHEN EXTRACT(HOUR FROM inicio_periodo) BETWEEN 16 AND 19 THEN '16:00 - 19:59'
      WHEN EXTRACT(HOUR
    FROM
      inicio_periodo) BETWEEN 20
    AND 23 THEN '20:00 - 23:59'
  END
    AS periodo
  FROM
    {{ ref('viagem_completa') }}
  WHERE
    DATA = "2024-05-26"
  GROUP BY
    consorcio,
    servico_realizado,
    periodo
  ORDER BY
    consorcio,
    servico_realizado,
    periodo )
SELECT
  p.trip_short_name,
  p.consorcio,
  COALESCE(SUM(CASE
        WHEN p.periodo = '00:00 - 03:59' THEN quantidade_viagens_planejadas
    END
      ), 0) AS `00:00 - 03:59 partidas_planejadas`,
  COALESCE(SUM(CASE
        WHEN p.periodo = '04:00 - 07:59' THEN quantidade_viagens_planejadas
    END
      ), 0) AS `04:00 - 07:59 partidas_planejadas`,
  COALESCE(SUM(CASE
        WHEN p.periodo = '08:00 - 11:59' THEN quantidade_viagens_planejadas
    END
      ), 0) AS `08:00 - 11:59 partidas_planejadas`,
  COALESCE(SUM(CASE
        WHEN p.periodo = '12:00 - 15:59' THEN quantidade_viagens_planejadas
    END
      ), 0) AS `12:00 - 15:59 partidas_planejadas`,
  COALESCE(SUM(CASE
        WHEN p.periodo = '16:00 - 19:59' THEN quantidade_viagens_planejadas
    END
      ), 0) AS `16:00 - 19:59 partidas_planejadas`,
  COALESCE(SUM(CASE
        WHEN p.periodo = '20:00 - 23:59' THEN quantidade_viagens_planejadas
    END
      ), 0) AS `20:00 - 23:59 partidas_planejadas`,
  COALESCE(SUM(CASE
        WHEN c.periodo = '00:00 - 03:59' THEN quantidade_viagens_completas
    END
      ), 0) AS `00:00 - 03:59 partidas_apuradas`,
  COALESCE(SUM(CASE
        WHEN c.periodo = '04:00 - 07:59' THEN quantidade_viagens_completas
    END
      ), 0) AS `04:00 - 07:59 partidas_apuradas`,
  COALESCE(SUM(CASE
        WHEN c.periodo = '08:00 - 11:59' THEN quantidade_viagens_completas
    END
      ), 0) AS `08:00 - 11:59 partidas_apuradas`,
  COALESCE(SUM(CASE
        WHEN c.periodo = '12:00 - 15:59' THEN quantidade_viagens_completas
    END
      ), 0) AS `12:00 - 15:59 partidas_apuradas`,
  COALESCE(SUM(CASE
        WHEN c.periodo = '16:00 - 19:59' THEN quantidade_viagens_completas
    END
      ), 0) AS `16:00 - 19:59 partidas_apuradas`,
  COALESCE(SUM(CASE
        WHEN c.periodo = '20:00 - 23:59' THEN quantidade_viagens_completas
    END
      ), 0) AS `20:00 - 23:59 partidas_apuradas`,
  COALESCE(SUM(CASE
        WHEN p.periodo = '00:00 - 03:59' THEN ROUND(quantidade_viagens_completas * 100.0 / NULLIF(quantidade_viagens_planejadas, 0), 2)
    END
      ), 0) AS `00:00 - 03:59 perc_realizado`,
  COALESCE(SUM(CASE
        WHEN p.periodo = '04:00 - 07:59' THEN ROUND(quantidade_viagens_completas * 100.0 / NULLIF(quantidade_viagens_planejadas, 0), 2)
    END
      ), 0) AS `04:00 - 07:59 perc_realizado`,
  COALESCE(SUM(CASE
        WHEN p.periodo = '08:00 - 11:59' THEN ROUND(quantidade_viagens_completas * 100.0 / NULLIF(quantidade_viagens_planejadas, 0), 2)
    END
      ), 0) AS `08:00 - 11:59 perc_realizado`,
  COALESCE(SUM(CASE
        WHEN p.periodo = '12:00 - 15:59' THEN ROUND(quantidade_viagens_completas * 100.0 / NULLIF(quantidade_viagens_planejadas, 0), 2)
    END
      ), 0) AS `12:00 - 15:59 perc_realizado`,
  COALESCE(SUM(CASE
        WHEN p.periodo = '16:00 - 19:59' THEN ROUND(quantidade_viagens_completas * 100.0 / NULLIF(quantidade_viagens_planejadas, 0), 2)
    END
      ), 0) AS `16:00 - 19:59 perc_realizado`,
  COALESCE(SUM(CASE
        WHEN p.periodo = '20:00 - 23:59' THEN ROUND(quantidade_viagens_completas * 100.0 / NULLIF(quantidade_viagens_planejadas, 0), 2)
    END
      ), 0) AS `20:00 - 23:59 perc_realizado`,
  SUM(quantidade_viagens_completas) AS total_apur,
  SUM(quantidade_viagens_planejadas) AS total_plan
FROM
  viagem_planejada_hora p
LEFT JOIN
  viagem_completa_hora c
USING
  (trip_short_name)
GROUP BY
  p.trip_short_name,
  p.consorcio
ORDER BY
  p.trip_short_name,
  p.consorcio