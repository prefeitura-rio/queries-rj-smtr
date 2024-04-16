{{ 
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type": "date",
      "granularity":"day"
    },
    unique_key="id_registro",
    incremental_strategy="merge",
    merge_update_columns=["data", "datetime_registro", "id_registro", "id_veiculo", "servico", "link_foto", "validacao"],
    alias='sppo_registro_agente_verao'
  )
}}

{% if execute %}
  {% set ultima_data_agente_verao = run_query("SELECT MAX(data) FROM " ~ ref('sppo_registro_agente_verao_staging'))[0][0] %}
{% endif %}


SELECT
  * EXCEPT(rn)
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY id_registro ORDER BY datetime_captura DESC) AS rn
    FROM
      {{ ref('sppo_registro_agente_verao_staging') }}
    WHERE
      data = DATE('{{ ultima_data_agente_verao }}')
      AND validacao = TRUE
  )
WHERE rn = 1
