{{ 
  config(
      materialized='incremental',
      partition_by={
            "field":"data_transacao",
            "data_type": "date",
            "granularity":"day"
      }
  )
}}
SELECT
    *
FROM {{ref('rho_registros_sppo_view')}}

{% if is_incremental() %}
WHERE
    data_particao = "{{var('partition_date')}}"
{% endif %}