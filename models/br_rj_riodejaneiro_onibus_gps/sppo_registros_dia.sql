{{ 
  config(
      materialized='table',
      partition_by={
            "field":"data",
            "data_type": "date",
            "granularity":"day"
      }
  )
}}

SELECT
  EXTRACT(date
  FROM
    timestamp_gps) AS data,
  COUNT(*) AS total
FROM
  `rj-smtr.br_rj_riodejaneiro_onibus_gps.sppo_registros`
WHERE
  data >= DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 30 day)
  AND timestamp_gps <= DATETIME_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 1 hour)
GROUP BY
    1