{{
  config(
    alias='contato_pessoa_juridica',
  )
}}

WITH 
    contato_pessoa_juridica AS (
        SELECT
            data,
            SAFE_CAST(NR_SEQ_CONTATO AS STRING) AS nr_seq_contato,
            SAFE_CAST(CD_CLIENTE AS STRING) AS cd_cliente,
            timestamp_captura,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS datetime_inclusao,
            SAFE_CAST(JSON_VALUE(content, '$.NM_CONTATO') AS STRING) AS nm_contato,
            SAFE_CAST(JSON_VALUE(content, '$.NR_RAMAL') AS STRING) AS nr_ramal,
            SAFE_CAST(JSON_VALUE(content, '$.NR_TELEFONE') AS STRING) AS nr_telefone,
            SAFE_CAST(JSON_VALUE(content, '$.TX_EMAIL') AS STRING) AS tx_email,
        FROM
            {{ source("br_rj_riodejaneiro_bilhetagem_staging", "contato_pessoa_juridica") }}
    ),
    contato_pessoa_juridica_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY nr_seq_contato, cd_cliente ORDER BY timestamp_captura DESC) AS rn
        FROM
            contato_pessoa_juridica
    )
SELECT
  * EXCEPT(rn)
FROM
  contato_pessoa_juridica_rn
WHERE
  rn = 1