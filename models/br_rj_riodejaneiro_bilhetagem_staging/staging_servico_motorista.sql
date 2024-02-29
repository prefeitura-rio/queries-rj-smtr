{{
  config(
    alias='servico_motorista',
  )
}}

SELECT
  data,
  SAFE_CAST(NR_LOGICO_MIDIA AS STRING) AS nr_logico_midia,
  SAFE_CAST(ID_SERVICO AS STRING) AS id_servico,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo") AS timestamp_captura,
  SAFE_CAST(JSON_VALUE(content, '$.CD_LINHA') AS STRING) AS cd_linha,
  SAFE_CAST(JSON_VALUE(content, '$.CD_OPERADORA') AS STRING) AS cd_operadora,
  SAFE_CAST(JSON_VALUE(content, '$.CD_STATUS') AS STRING) AS cd_status,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_ABERTURA') AS STRING)), 'America/Sao_Paulo') AS dt_abertura,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_FECHAMENTO') AS STRING)), 'America/Sao_Paulo') AS dt_fechamento,
  SAFE_CAST(JSON_VALUE(content, '$.ID_VEICULO') AS STRING) AS id_veiculo,
  SAFE_CAST(JSON_VALUE(content, '$.NR_LOGICO_MIDIA_FECHAMENTO') AS STRING) AS nr_logico_midia_fechamento,
  SAFE_CAST(JSON_VALUE(content, '$.SN_DEVICE') AS STRING) AS sn_device,
  SAFE_CAST(JSON_VALUE(content, '$.TP_GERACAO') AS STRING) AS tp_geracao,
  SAFE_CAST(JSON_VALUE(content, '$.VL_TARIFA_LINHA') AS FLOAT64) AS vl_tarifa_linha
FROM
  {{ source('br_rj_riodejaneiro_bilhetagem_staging', 'servico_motorista') }}