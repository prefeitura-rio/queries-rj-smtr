{{
  config(
    alias='rho_registros_stpl',
  )
}}

SELECT 
    SAFE_CAST(operadora AS STRING) operadora,
    SAFE_CAST(linha AS STRING) linha,
    SAFE_CAST(data_transacao AS DATE) data_transacao,
    -- SAFE_CAST(PARSE_DATETIME("%Y-%m-%d", data_transacao) AS DATETIME) data_transacao,
    SAFE_CAST(hora_transacao AS INT64) hora_transacao,
    SAFE_CAST(total_gratuidades AS INT64) total_gratuidades,
    SAFE_CAST(total_pagantes AS INT64) total_pagantes,
    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
    SAFE_CAST(ano AS INT64) ano,
    SAFE_CAST(mes AS INT64) mes,
    SAFE_CAST(dia AS INT64) dia,
    DATE(CONCAT(ano,'-', mes, '-', dia)) data_particao
from 
    {{ source("br_rj_riodejaneiro_rdo_staging", "rho5_registros_stpl") }} as t