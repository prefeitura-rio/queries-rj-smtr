
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
      *
    FROM
      {{ ref("infracao_iplanrio_stu") }}
    {% if is_incremental() %}
      {%- if execute %}
        {% set infracao_date = run_query("SELECT MIN(data) FROM " ~ ref("infracao_iplanrio_stu") ~ " WHERE data >= DATE_ADD(DATE('" ~ var("run_date") ~ "'), INTERVAL 7 DAY)").columns[0].values()[0] %}
      {% endif -%}
    WHERE
      data = DATE("{{ infracao_date }}")
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