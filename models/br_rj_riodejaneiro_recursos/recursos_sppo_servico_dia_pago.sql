{{ config(
  materialized = "table",
) }}

WITH
  -- 1. Trata os IDs de recurso de forma que, caso tenha mais que um, sempre estejam separados por vírgula, ordenados do menor para o maior e sem espaços adicionais
  treated_id_recurso AS (
  SELECT
    DISTINCT *
  FROM (
    SELECT
      id_recurso_old,
      ARRAY_TO_STRING(ARRAY(
        SELECT
          TRIM(id)
        FROM
          UNNEST(id_array) AS id
        ORDER BY
          id), ", ") AS id_recurso
    FROM (
      SELECT
        id_recurso AS id_recurso_old,
        SPLIT(REPLACE(id_recurso, " e ", ", "), ", ") AS id_array
      FROM
        {{ source("br_rj_riodejaneiro_recursos_staging", "recursos_sppo_servico_dia_pago") }}
  ))),
  -- 2. Trata a tabela em staging
  treated_recurso AS (
  SELECT
    * EXCEPT(DATA,
      valor_pago,
      tipo_dia),
    PARSE_DATE("%d/%m/%Y", DATA) AS DATA,
    SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(valor_pago, r"\.", ""), r",", "."), r"[^\d\.-]", "") AS FLOAT64) AS valor_pago,
    CASE
      WHEN tipo_dia LIKE "R$%" OR tipo_dia = "" THEN NULL
    ELSE
    tipo_dia
  END
    AS tipo_dia
  FROM
    {{ source("br_rj_riodejaneiro_recursos_staging", "recursos_sppo_servico_dia_pago") }}
  WHERE
    DATA NOT LIKE "%-%" )
SELECT
  data,
  tipo_dia,
  i.id_recurso,
  tipo_recurso,
  quinzena_ocorrencia,
  quinzena_pagamento,
  consorcio,
  servico,
  valor_pago
FROM
  treated_recurso AS t
LEFT JOIN
  treated_id_recurso AS i
ON
  t.id_recurso = i.id_recurso_old