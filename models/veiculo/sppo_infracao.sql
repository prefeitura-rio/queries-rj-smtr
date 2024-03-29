{{ 
  config(
    materialized='incremental',
    partition_by={
      "field":"data",
      "data_type": "date",
      "granularity":"day"
    },
    incremental_strategy='insert_overwrite'
  )
}}

WITH 
  infracao AS (
    SELECT
      *,
    FROM
      {{ ref("infracao_iplanrio_stu") }}
    {% if is_incremental() %}
    WHERE
      data = DATE_ADD(DATE("{{ var('run_date') }}"), INTERVAL {{ var('infracao_limite_maximo_dias') }} DAY) 
      -- TODO: separar pipeline de infracao/licenciamento da apuração do subsídio (run_date = hoje)
    {% endif %}
  ),
  infracao_rn AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY data, id_auto_infracao) AS rn
    FROM
      infracao
  )
SELECT
  * EXCEPT(rn)
FROM
  infracao_rn
WHERE
  rn = 1