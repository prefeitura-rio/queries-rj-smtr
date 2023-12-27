{{ config(
  materialized="view"
) }} 

WITH
  data_versao AS (
  SELECT
    data_versao,
    data_versao AS data_inicio,
    COALESCE(DATE_SUB(LEAD(data_versao) OVER (ORDER BY data_versao), INTERVAL 1 DAY), LAST_DAY(data_versao, MONTH)) AS data_fim
  FROM (
    SELECT
      DISTINCT data_versao,
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
  (data_versao,
    tipo_dia)