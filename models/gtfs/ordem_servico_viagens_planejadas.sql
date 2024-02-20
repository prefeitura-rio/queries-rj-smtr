{{ config(
  materialized="view"
) }} 

WITH
  data_versao AS (
  SELECT
    feed_start_date,
    feed_start_date AS data_inicio,
    COALESCE(DATE_SUB(LEAD(feed_start_date) OVER (ORDER BY feed_start_date), INTERVAL 1 DAY), LAST_DAY(feed_start_date, MONTH)) AS data_fim
  FROM (
    SELECT
      DISTINCT feed_start_date,
    FROM
      {{ ref("ordem_servico_gtfs") }} )),
  subsidio_data_versao_efetiva AS (
  SELECT
    * EXCEPT(tipo_dia),
    SPLIT(tipo_dia, " - ")[0] AS tipo_dia
  FROM
    {{ ref("subsidio_data_versao_efetiva") }} )
SELECT
  DATA,
  sd.tipo_dia,
  servico,
  viagens_planejadas
FROM
  UNNEST(GENERATE_DATE_ARRAY((SELECT MIN(data_inicio) FROM data_versao), (SELECT MAX(data_fim) FROM data_versao))) AS DATA
LEFT JOIN
  data_versao AS d
ON
  DATA BETWEEN d.data_inicio
  AND d.data_fim
LEFT JOIN
  subsidio_data_versao_efetiva AS sd
USING
  (DATA)
LEFT JOIN
  {{ ref("ordem_servico_gtfs") }} AS o
USING
  (feed_start_date,
    tipo_dia)
