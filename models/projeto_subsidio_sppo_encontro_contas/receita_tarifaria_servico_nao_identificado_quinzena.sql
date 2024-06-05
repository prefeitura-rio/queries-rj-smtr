{% if var("encontro_contas_modo") == "_pre_gt" %}
{{ config(alias=this.name ~ var('encontro_contas_modo')) }}
WITH
  q1 AS (
  SELECT
    FORMAT_DATE('%Y-%m-Q1', date) AS quinzena,
    date AS data_inicial_quinzena,
    DATE_ADD(date, INTERVAL 14 DAY) AS data_final_quinzena
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2022-06-01', '2023-12-31', INTERVAL 1 MONTH)) AS date ),
  q2 AS (
  SELECT
    FORMAT_DATE('%Y-%m-Q2', date) AS quinzena,
    DATE_ADD(date, INTERVAL 15 DAY) AS data_inicial_quinzena,
    LAST_DAY(date) AS data_final_quinzena
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2022-06-01', '2023-12-31', INTERVAL 1 MONTH)) AS date ),
  quinzenas AS (
  SELECT
    *
  FROM
    q1
  UNION ALL
  SELECT
    *
  FROM
    q2
  ORDER BY
    data_inicial_quinzena )
SELECT
  quinzena,
  data_inicial_quinzena,
  data_final_quinzena,
  consorcio_rdo,
  servico_tratado_rdo,
  linha_rdo,
  tipo_servico_rdo,
  ordem_servico_rdo,
  COUNT(data_rdo) AS quantidade_dias_rdo,
  SUM(receita_tarifaria_aferida_rdo) AS receita_tarifaria_aferida_rdo
FROM
  quinzenas qz
LEFT JOIN (
  SELECT * from {{ ref("aux_balanco_rdo_servico_dia") }} WHERE servico is null
) bs
ON
  bs.data_rdo BETWEEN qz.data_inicial_quinzena
  AND qz.data_final_quinzena
GROUP BY
  1,2,3,4,5,6,7,8
ORDER BY
  2,4,5,6,7,8
{% else %}
{{ config(enabled=false) }}
{% endif %}