
{{
  config(
    materialized='view',
    alias='sppo_realocacao'
  )
}}

SELECT 
  SAFE_CAST(id_veiculo AS STRING) id_veiculo,
  SAFE_CAST(DATETIME(TIMESTAMP(datetime_operacao), "America/Sao_Paulo") AS DATETIME) datetime_operacao,
  concat(
    ifnull(REGEXP_EXTRACT(servico, r'[A-Z]+'), ""), 
    ifnull(REGEXP_EXTRACT(servico, r'[0-9]+'), "") 
  ) as servico,
  SAFE_CAST(DATETIME(TIMESTAMP(datetime_entrada), "America/Sao_Paulo") AS DATETIME) as datetime_entrada,
  SAFE_CAST(DATETIME(TIMESTAMP(datetime_saida), "America/Sao_Paulo") AS DATETIME) as datetime_saida,
  SAFE_CAST(DATETIME(TIMESTAMP(timestamp_processamento), "America/Sao_Paulo") AS DATETIME) as timestamp_processamento,
  SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) as timestamp_captura,
  SAFE_CAST(data AS DATE) as data,
  SAFE_CAST(hora AS INT64) as hora
FROM
  {{ source('br_rj_riodejaneiro_onibus_gps_zirix_staging', "realocacao") }}