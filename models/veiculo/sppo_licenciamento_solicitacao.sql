{{ config(
       materialized='incremental',
       partition_by={
              "field":"data",
              "data_type": "date",
              "granularity":"day"
       },
       unique_key=['data', 'id_veiculo'],
       incremental_strategy='insert_overwrite'
)
}}

SELECT
   *
 FROM
     {{ ref('sppo_licenciamento_solicitacao_staging') }} as t