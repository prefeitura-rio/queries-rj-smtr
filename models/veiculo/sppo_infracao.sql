
 {{ config(
       materialized='incremental',
       partition_by={
              "field":"data",
              "data_type": "date",
              "granularity":"day"
       },
       unique_key=['data', 'id_auto_infracao'],
       incremental_strategy='insert_overwrite'
)
}}

{%- if execute %}
  {% set infracao_date = run_query("SELECT MIN(SAFE_CAST(data AS DATE)) FROM " ~ ref('sppo_infracao_staging') ~ " WHERE SAFE_CAST(data AS DATE) >= DATE_ADD(DATE('" ~ var("run_date") ~ "'), INTERVAL 7 DAY)").columns[0].values()[0] %}
{% endif -%}

WITH 
  infracao AS (
    SELECT
      *
    FROM
      {{ ref("sppo_infracao_staging") }} as t
    WHERE
      SAFE_CAST(data AS DATE) = DATE("{{ infracao_date }}")
  ),
  infracao_rn AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY data, id_auto_infracao) rn
    FROM
      infracao
  )
SELECT
  * EXCEPT(rn)
FROM
  infracao_rn
WHERE
  rn = 1