
{{ config(
        materialized='view',
        alias='sppo_infracao'
  ) 
}}


SELECT
  SAFE_CAST(DATA AS DATE) data,
  SAFE_CAST(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP(timestamp_captura), SECOND), "America/Sao_Paulo" ) AS DATETIME) timestamp_captura,
  SAFE_CAST(JSON_VALUE(content,'$.modo') AS STRING) modo,
  SAFE_CAST(JSON_VALUE(content,'$.permissao') AS STRING) permissao,
  SAFE_CAST(placa AS STRING) placa,
  SAFE_CAST(id_auto_infracao AS STRING) id_auto_infracao,
  PARSE_DATE("%d/%m/%Y", SAFE_CAST(JSON_VALUE(content,'$.data_infracao') AS STRING)) data_infracao,
  SAFE_CAST(JSON_VALUE(content,'$.valor') AS FLOAT64) valor,
  SAFE_CAST(JSON_VALUE(content,'$.id_infracao') AS STRING) id_infracao,
  SAFE_CAST(JSON_VALUE(content,'$.infracao') AS STRING) infracao,
  SAFE_CAST(JSON_VALUE(content,'$.status') AS STRING) status,
  IF(JSON_VALUE(content,'$.data_pagamento') = "", NULL, PARSE_DATE("%d/%m/%Y", JSON_VALUE(content,'$.data_pagamento'))) data_pagamento
FROM
  {{ source('veiculo_staging','sppo_infracao') }} as t

