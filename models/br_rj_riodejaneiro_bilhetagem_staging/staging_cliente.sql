{{
  config(
    alias='cliente',
  )
}}

WITH 
    cliente AS (
        SELECT
            data,
            SAFE_CAST(CD_CLIENTE AS STRING) AS cd_cliente,
            timestamp_captura,
            SAFE_CAST(JSON_VALUE(content, '$.CD_TIPO_DOCUMENTO') AS STRING) AS cd_tipo_documento,
            SAFE_CAST(JSON_VALUE(content, '$.NM_CLIENTE') AS STRING) AS nm_cliente,
            SAFE_CAST(JSON_VALUE(content, '$.NM_CLIENTE_SOCIAL') AS STRING) AS nm_cliente_social,
            SAFE_CAST(JSON_VALUE(content, '$.IN_TIPO_PESSOA_FISICA_JURIDICA') AS STRING) AS in_tipo_pessoa_fisica_juridica,
            SAFE_CAST(JSON_VALUE(content, '$.NR_DOCUMENTO') AS STRING) AS nr_documento,
            SAFE_CAST(JSON_VALUE(content, '$.NR_DOCUMENTO_ALTERNATIVO') AS STRING) AS nr_documento_alternativo,
            SAFE_CAST(JSON_VALUE(content, '$.TX_EMAIL') AS STRING) AS tx_email,
            SAFE_CAST(JSON_VALUE(content, '$.NR_TELEFONE') AS STRING) AS nr_telefone,
            SAFE_CAST(JSON_VALUE(content, '$.DT_CADASTRO') AS STRING) AS dt_cadastro
        FROM
            {{ source("br_rj_riodejaneiro_bilhetagem_staging", "cliente") }}
    ),
    cliente_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cd_cliente ORDER BY timestamp_captura DESC) AS rn
        FROM
            cliente
    )
SELECT
  * EXCEPT(rn)
FROM
  cliente_rn
WHERE
  rn = 1