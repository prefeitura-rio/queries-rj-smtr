-- 1. Realiza join de datas com a tabela calendar, agregando data_versao e service_id
WITH aux_calendar AS (
  SELECT
    c.timestamp_captura,
    data,
    c.service_id
  FROM UNNEST(GENERATE_DATE_ARRAY("2022-06-01", "2022-12-31")) AS data
  LEFT JOIN
    {{ var("gtfs_calendar") }} AS c
  ON
    data BETWEEN c.start_date AND c.end_date
    AND 
      CASE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 1 AND c.sunday      = 1 THEN TRUE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 2 AND c.monday      = 1 THEN TRUE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 3 AND c.tuesday     = 1 THEN TRUE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 4 AND c.wednesday   = 1 THEN TRUE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 5 AND c.thursday    = 1 THEN TRUE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 6 AND c.friday      = 1 THEN TRUE
        WHEN EXTRACT(DAYOFWEEK FROM data) = 7 AND c.saturday    = 1 THEN TRUE
        ELSE FALSE
      END
),
-- 2. Realiza join de datas com a tabela calendar_dates, agregando service_id de exceções (feriados)
aux_calendar_dates AS (
  SELECT
    c.* EXCEPT(service_id),
  CASE
    WHEN d.service_id IS NOT NULL THEN d.service_id
    ELSE c.service_id
  END AS service_id
  FROM
    aux_calendar AS c
  LEFT JOIN
    {{ var("gtfs_calendar_dates") }} AS d
  ON
    d.date = c.data
  AND
    d.timestamp_captura = c.timestamp_captura
  AND 
    d.exception_type = "1"
)

SELECT
  *
FROM aux_calendar_dates
WHERE timestamp_captura IS NOT NULL