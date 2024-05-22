
SELECT
    PARSE_DATE('%d/%m/%Y', SAFE_CAST(JSON_VALUE(content, '$.data') AS STRING)) AS data,
    SAFE_CAST(JSON_VALUE(content, '$.lancamento') AS STRING) AS lancamento,
    SAFE_CAST(JSON_VALUE(content, '$.operacao') AS STRING) AS operacao,
    SAFE_CAST(JSON_VALUE(content, '$.tipo') AS STRING) AS tipo,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(JSON_VALUE(content, '$.valor'), 'R$', ''), '.', ''), ',', '.') AS FLOAT64) AS valor,
    SAFE_CAST(REPLACE(REPLACE(REPLACE(JSON_VALUE(content, '$.saldo_final'), 'R$', ''), '.', ''), ',', '.') AS FLOAT64) AS saldo_final,
    SAFE_CAST(JSON_VALUE(content, '$.favorecido') AS STRING) AS favorecido,
    SAFE_CAST(JSON_VALUE(content, '$.modal') AS STRING) AS modal
FROM
    {{source("controle_financeiro_staging", "CB")}}


