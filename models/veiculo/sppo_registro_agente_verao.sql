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
  )
}}

SELECT
  *
FROM
  {{ ref('sppo_registro_agente_verao_staging') }}
