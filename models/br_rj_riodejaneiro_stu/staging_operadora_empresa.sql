{{
  config(
    alias='operadora_empresa',
  )
}}

WITH 
    operadora_empresa AS (
        SELECT
            data,
            SAFE_CAST(Perm_Autor AS STRING) AS perm_autor,
            timestamp_captura,
            SAFE_CAST(JSON_VALUE(content, '$.CNPJ') AS STRING) AS cnpj,
            DATE(PARSE_TIMESTAMP('%d/%m/%Y', SAFE_CAST(JSON_VALUE(content, '$.Data') AS STRING)), "America/Sao_Paulo") AS data_registro,
            SAFE_CAST(JSON_VALUE(content, '$.Processo') AS STRING) AS processo,
            SAFE_CAST(JSON_VALUE(content, '$.Razao_Social') AS STRING) AS razao_social,
            SAFE_CAST(JSON_VALUE(content, '$.id_modo') AS STRING) AS id_modo,
            SAFE_CAST(JSON_VALUE(content, '$.modo') AS STRING) AS modo,
            SAFE_CAST(JSON_VALUE(content, '$.tipo_permissao') AS STRING) AS tipo_permissao
        FROM
            {{ source("br_rj_riodejaneiro_stu_staging", "operadora_empresa") }}
    ),
    operadora_empresa_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY COALESCE(cnpj, perm_autor), modo ORDER BY timestamp_captura DESC, data_registro DESC) AS rn
        FROM
            operadora_empresa
    )
SELECT
  * EXCEPT(rn)
FROM
  operadora_empresa_rn
WHERE
  rn = 1