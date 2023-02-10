-- TODO: configurar materializacao da tabela
 SELECT
   data,
   timestamp_captura,
   SAFE_CAST(placa AS STRING) placa,
   SAFE_CAST(id_auto_infracao AS STRING) id_auto_infracao,
   SAFE_CAST(JSON_VALUE(content,'$.permissao') AS STRING) permissao,
   SAFE_CAST(JSON_VALUE(content,'$.modo') AS STRING) modo,
   PARSE_DATE("%d/%m/%Y", SAFE_CAST(JSON_VALUE(content,'$.data_infracao') AS STRING)) data_infracao,
   SAFE_CAST(JSON_VALUE(content,'$.valor') AS FLOAT64) valor,
   SAFE_CAST(JSON_VALUE(content,'$.id_infracao') AS STRING) id_infracao,
   SAFE_CAST(JSON_VALUE(content,'$.infracao') AS STRING) infracao,
   SAFE_CAST(JSON_VALUE(content,'$.status') AS STRING) status,
   PARSE_DATE("%d/%m/%Y", SAFE_CAST(JSON_VALUE(content,'$.data_pagamento') AS STRING)) data_pagamento
 FROM
   {{ var('sppo_infracao_staging') }} as t