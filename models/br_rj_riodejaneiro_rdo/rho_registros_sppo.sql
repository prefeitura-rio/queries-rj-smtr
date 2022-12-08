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
    SAFE_CAST(linha AS STRING) linha,
    SAFE_CAST(data_transacao AS DATE) data_transacao,
    SAFE_CAST(hora_transacao AS INT64) hora_transacao,
    SAFE_CAST(total_gratuidades AS INT64) total_gratuidades,
    SAFE_CAST(total_pagantes_especie AS INT64) total_pagantes_especie,
    SAFE_CAST(total_pagantes_cartao AS INT64) total_pagantes_cartao,
    SAFE_CAST(registro_processado AS STRING) registro_processado,
    SAFE_CAST(data_processamento AS DATE) data_processamento,
    SAFE_CAST(operadora AS STRING) operadora,
    SAFE_CAST(linha_rcti AS STRING) linha_rcti,
    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
FROM {{var('rho_registros_sppo_staging')}}
WHERE  date_diff(data_processamento, data_transacao, DAY) <= {{var('rho_max_processing_interval')}}
{%% if {% if is_incremental() %}
AND
    data between DATE("{{var('date_range_start')}}") and DATE("{{var('date_range_end')}}")
{% endif %}%}