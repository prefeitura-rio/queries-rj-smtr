{{
  config(
    materialized="ephemeral"
  )
}}

/*
Tabela auxiliar para incorporar informações adicionais da Coordenadoria Geral de Licenciamento e Fiscalização (TR/SUBTT/CGLF)
*/

SELECT
  SAFE_CAST(DATA AS DATE) AS data,
  SAFE_CAST(id_veiculo AS STRING) AS id_veiculo,
  SAFE_CAST(placa AS STRING) AS placa,
  SAFE_CAST(JSON_VALUE(content,"$.permissao") AS STRING) AS permissao,
  SAFE_CAST(JSON_VALUE(content,"$.chassi") AS STRING) AS chassi,
  SAFE_CAST(JSON_VALUE(content,"$.ano_fabricacao") AS INT64) AS ano_fabricacao,
  SAFE_CAST(JSON_VALUE(content,"$.selo") AS STRING) AS selo,
  SAFE_CAST(JSON_VALUE(content,"$.darm") AS STRING) AS darm,
  SAFE_CAST(JSON_VALUE(content,"$.guia") AS STRING) AS guia,
  SAFE_CAST(JSON_VALUE(content,"$.data_ultima_vistoria") AS DATE) AS data_ultima_vistoria,
  SAFE_CAST(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP(timestamp_captura), SECOND), "America/Sao_Paulo" ) AS DATETIME) timestamp_captura,
FROM
  {{ source("veiculo_staging", "sppo_licenciamento_cglf_aux") }} AS t