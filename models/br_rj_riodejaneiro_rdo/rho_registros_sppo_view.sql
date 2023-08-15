SELECT
    CONCAT(TRIM(linha), '_', data_transacao, '_', hora_transacao) id_transacao,
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
    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
    DATE(CONCAT(ano,'-', mes, '-', dia)) data_particao
FROM {{var('rho_registros_sppo_staging')}}
WHERE data_transacao > "{{var('rho_sppo_start_date')}}"