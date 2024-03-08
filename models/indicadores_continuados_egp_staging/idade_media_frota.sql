{{
  config( 
    partition_by = { 
    "field": "data",
    "data_type": "date",
    "granularity": "month"
    },
)}}

WITH
  -- 1. Seleciona a última data disponível de cada mês
  datas AS (
  SELECT
    EXTRACT(MONTH FROM DATA) AS mes,
    EXTRACT(YEAR FROM DATA) AS ano,
    MAX(DATA) AS DATA
  FROM
    {{ ref("sppo_licenciamento") }}
  WHERE
    data < DATE_TRUNC(CURRENT_DATE(), MONTH)
  GROUP BY
    1,
    2),
  -- 2. Calcula a idade de todos os veículos para a data de referência
  idade_frota AS (
  SELECT
    DATA,
    EXTRACT(YEAR FROM DATA) - CAST(ano_fabricacao AS INT64) AS idade
  FROM
    datas AS d
  LEFT JOIN
    {{ ref("sppo_licenciamento") }} AS l
  USING
    (DATA))
-- 3. Calcula a idade média
SELECT
  DATA,
  EXTRACT(YEAR FROM DATA) AS ano,
  EXTRACT(MONTH FROM DATA) AS mes,
  ROUND(AVG(idade),2) AS idade_media_veiculos_mes
FROM
  idade_frota
GROUP BY
  1,
  2,
  3
