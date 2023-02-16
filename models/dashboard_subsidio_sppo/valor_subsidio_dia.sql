WITH
  classificacao AS (
  SELECT
    data,
    id_classificacao
  FROM
    {{ ref("subsidio_data_versao_efetiva") }}
  CROSS JOIN
    UNNEST(GENERATE_ARRAY(1, 4, 1)) AS id_classificacao )
SELECT
  *,
  CASE
    WHEN id_classificacao = 1 THEN 0
    WHEN id_classificacao = 2 THEN 1.97
    WHEN id_classificacao = 3 THEN 0
    WHEN id_classificacao = 4 THEN 2.81
    ELSE 0
  END AS valor_subsidio_por_km
FROM
  classificacao