{{
  config(
    materialized='view',
    alias='sppo_registros'
  )
}}

SELECT 
    SAFE_CAST(ordem AS STRING) ordem,
    SAFE_CAST(REPLACE(latitude, ',', '.') AS FLOAT64) latitude,
    SAFE_CAST(REPLACE(longitude, ',', '.') AS FLOAT64) longitude,
    SAFE_CAST(DATETIME(TIMESTAMP(datahora), "America/Sao_Paulo") AS DATETIME) timestamp_gps,
    SAFE_CAST(velocidade AS INT64) velocidade,
    concat(
        ifnull(REGEXP_EXTRACT(linha, r'[A-Z]+'), ""), 
        ifnull(REGEXP_EXTRACT(linha, r'[0-9]+'), "") 
    ) as linha,
    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
    SAFE_CAST(data AS DATE) data,
    SAFE_CAST(hora AS INT64) hora
from 
    {{ source('br_rj_riodejaneiro_onibus_gps_zirix_staging', "registros")}}