WITH
  content_data AS (
  SELECT
    CONCAT("[",content,"]") AS json_content,
    timestamp_captura
  FROM
    {{source("controle_financeiro_staging", "cett")}} 
)
SELECT
  PARSE_DATE('%d/%m/%Y', SAFE_CAST(JSON_VALUE(content, '$.Data') AS STRING)) AS data,
  SAFE_CAST(JSON_VALUE(content, '$.Lançamento') AS STRING) AS lancamento,
  SAFE_CAST(JSON_VALUE(content, '$.Operação') AS STRING) AS operacao,
  SAFE_CAST(JSON_VALUE(content, '$.Tipo') AS STRING) AS tipo,
  SAFE_CAST(REPLACE(REPLACE(REPLACE(JSON_VALUE(content, '$.Valor'), 'R$ ', ''), '.', ''), ',', '.') AS FLOAT64) AS valor,
  SAFE_CAST(REPLACE(REPLACE(REPLACE(JSON_VALUE(content, '$.Saldo Final'), 'R$ ', ''), '.', ''), ',', '.') AS FLOAT64) AS saldo_final,
  SAFE_CAST(JSON_VALUE(content, '$.Favorecido') AS STRING) AS favorecido,
  SAFE_CAST(JSON_VALUE(content, '$.Modal') AS STRING) AS modal,
  timestamp(timestamp_captura) AS timestamp_captura
FROM
  content_data,
  UNNEST(JSON_EXTRACT_ARRAY(json_content)) AS content