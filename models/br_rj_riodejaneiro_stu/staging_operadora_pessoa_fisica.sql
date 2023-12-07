{{
  config(
    alias='operadora_pessoa_fisica',
  )
}}

WITH 
    operadora_pessoa_fisica AS (
        SELECT
            data,
            SAFE_CAST(Perm_Autor AS STRING) AS perm_autor,
            timestamp_captura,
            SAFE_CAST(JSON_VALUE(content, '$.CPF') AS STRING) AS cpf,
            PARSE_DATE('%d/%m/%Y', LEFT(SAFE_CAST(JSON_VALUE(content, '$.Data') AS STRING), 10)) AS data_registro,
            SAFE_CAST(JSON_VALUE(content, '$.Ratr') AS STRING) AS ratr,
            SAFE_CAST(JSON_VALUE(content, '$.Processo') AS STRING) AS processo,
            SAFE_CAST(JSON_VALUE(content, '$.Nome') AS STRING) AS nome,
            SAFE_CAST(JSON_VALUE(content, '$.Placa') AS STRING) AS placa,
            SAFE_CAST(JSON_VALUE(content, '$.modo') AS STRING) AS modo,
            SAFE_CAST(JSON_VALUE(content, '$.tipo_permissao') AS STRING) AS tipo_permissao
        FROM
            {{ source("br_rj_riodejaneiro_stu_staging", "operadora_pessoa_fisica") }}
    ),
    operadora_pessoa_fisica_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY COALESCE(cpf, perm_autor), modo ORDER BY timestamp_captura DESC, data_registro DESC) AS rn
        FROM
            operadora_pessoa_fisica
    )
SELECT
  * EXCEPT(rn)
FROM
  operadora_pessoa_fisica_rn
WHERE
  rn = 1