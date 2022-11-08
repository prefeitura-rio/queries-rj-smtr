-- 1. Realiza join de datas com a tabela calendar, agregando data_versao e service_id
WITH aux_calendar AS (
  SELECT
    c.start_date AS data_versao,
    data,
    c.service_id
  FROM UNNEST(GENERATE_DATE_ARRAY("2022-06-01", "2022-12-31")) AS data
  LEFT JOIN
    `rj-smtr-dev.gtfs_test.calendar` AS c
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
    `rj-smtr-dev.gtfs_test.calendar_dates` AS d
  ON
    d.date = c.data
  AND
    d.data_versao = c.data_versao
  AND 
    d.exception_type = 1
)
-- 3. Inclui o valor do subsídio por km
SELECT
  a.*,
  CASE
    WHEN EXTRACT(MONTH FROM data) = 6   THEN 2.13
    WHEN EXTRACT(MONTH FROM data) = 7   THEN 1.84
    WHEN EXTRACT(MONTH FROM data) = 8   THEN 1.80
    WHEN EXTRACT(MONTH FROM data) = 9   THEN 1.75
    WHEN EXTRACT(MONTH FROM data) = 10  THEN 1.62
    WHEN EXTRACT(MONTH FROM data) = 11  THEN 1.53
    WHEN EXTRACT(MONTH FROM data) = 12  THEN 1.78
  END AS valor_subsidio_km
FROM aux_calendar_dates AS a
ORDER BY data DESC