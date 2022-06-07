SELECT 
    SAFE_CAST(id_veiculo AS STRING) id_veiculo,
    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_gps), "America/Sao_Paulo" ) AS DATETIME) timestamp_gps,
    SAFE_CAST(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP(timestamp_captura), SECOND), "America/Sao_Paulo" ) AS DATETIME) timestamp_captura,
    REPLACE(content,"None","") content,
    data,
    hora
from 
    {{var('brt_registros_staging')}} as t