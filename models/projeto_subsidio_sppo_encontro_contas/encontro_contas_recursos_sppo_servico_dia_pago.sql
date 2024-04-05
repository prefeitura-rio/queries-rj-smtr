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
  -- Quando o valor do recurso pago for R$ 0, desconsidera-se o recurso, pois:
    -- Recurso pode ter sido cancelado (pago e depois revertido)
    -- Problema reporto não gerou impacto na operação (quando aparece apenas 1 vez)
  valor_pago != 0
  -- Desconsideram-se recursos do tipo "Algoritmo" (igual a apuração em produção, levantado pela TR/SUBTT/CMO) 
  -- Desconsideram-se recursos do tipo "Viagem Individual" (não afeta serviço-dia)
  AND tipo_recurso NOT IN ("Algoritmo", "Viagem Individual")
  -- Desconsideram-se recursos de reprocessamento que já constam em produção
  AND NOT (data BETWEEN "2022-06-01" AND "2022-06-30" 
            AND tipo_recurso = "Reprocessamento")