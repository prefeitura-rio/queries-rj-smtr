{{
  config(
    alias='rho_registros_sppo',
  )
}}


SELECT
    CONCAT(TRIM(linha_rcti), '_', data_transacao, '_', hora_transacao, ano,'_', mes, '_', dia) id_transacao,
    TRIM(linha) linha,
    SAFE_CAST(data_transacao AS DATE) data_transacao,
    SAFE_CAST(hora_transacao AS INT64) hora_transacao,
    SAFE_CAST(total_gratuidades AS INT64) total_gratuidades,
    SAFE_CAST(total_pagantes_especie AS INT64) total_pagantes_especie,
    SAFE_CAST(total_pagantes_cartao AS INT64) total_pagantes_cartao,
    SAFE_CAST(registro_processado AS STRING) registro_processado,
    SAFE_CAST(data_processamento AS DATE) data_processamento,
    SAFE_CAST(operadora AS STRING) operadora,
    SAFE_CAST(linha_rcti AS STRING) linha_rcti,
    DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS timestamp_captura,
    SAFE_CAST(ano AS INT64) ano,
    SAFE_CAST(mes AS INT64) mes,
    SAFE_CAST(dia AS INT64) dia,
    DATE(CONCAT(ano,'-', mes, '-', dia)) data_particao
FROM 
    {{ source("br_rj_riodejaneiro_rdo_staging", "rho_registros_sppo") }}