{{
  config(
    alias='pessoa_juridica',
  )
}}

WITH 
    pessoa_juridica AS (
        SELECT
            data,
            SAFE_CAST(CD_CLIENTE AS STRING) AS cd_cliente,
            timestamp_captura,
            SAFE_CAST(JSON_VALUE(content, '$.NM_RAZAO_SOCIAL') AS STRING) AS nm_razao_social,
            SAFE_CAST(JSON_VALUE(content, '$.NM_FANTASIA') AS STRING) AS nm_fantasia,
            SAFE_CAST(JSON_VALUE(content, '$.NR_INSCRICAO_ESTADUAL_MUNICIPAL') AS STRING) AS nr_inscricao_estadual_municipal,
            SAFE_CAST(JSON_VALUE(content, '$.TX_EMAIL_ALTERNATIVO') AS STRING) AS tx_email_alternativo,
            SAFE_CAST(JSON_VALUE(content, '$.NR_DOCUMENTO') AS STRING) AS nr_documento,
        FROM
            {{ source("br_rj_riodejaneiro_bilhetagem_staging", "pessoa_juridica") }}
    ),
    pessoa_juridica_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cd_cliente ORDER BY timestamp_captura DESC) AS rn
        FROM
            pessoa_juridica
    )
SELECT
  * EXCEPT(rn)
FROM
  pessoa_juridica_rn
WHERE
  rn = 1