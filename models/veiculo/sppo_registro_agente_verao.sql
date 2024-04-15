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
  *
FROM
  {{ ref('sppo_registro_agente_verao_staging') }}
WHERE
  ---------------- rever este filtro para particionamento ---------------------
  data = DATE('{{ ultima_data_agente_verao }}')
  AND validacao = TRUE