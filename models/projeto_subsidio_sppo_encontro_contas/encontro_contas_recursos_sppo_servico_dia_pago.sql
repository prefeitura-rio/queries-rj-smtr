{{
  config(
    materialized="ephemeral"
  )
}}

WITH
  recursos_sppo_servico_dia_pago_agg AS (
  SELECT
    data,
    id_recurso,
    tipo_recurso,
    consorcio,
    servico,
    SUM(valor_pago) AS valor_pago
  FROM
    {{ ref("recursos_sppo_servico_dia_pago") }}
  GROUP BY
    1,
    2,
    3,
    4,
    5)
SELECT
  *
FROM
  recursos_sppo_servico_dia_pago_agg
WHERE
  valor_pago != 0
  AND tipo_recurso != "Algoritmo"