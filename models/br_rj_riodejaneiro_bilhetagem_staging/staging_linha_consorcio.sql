{{
  config(
    alias='linha_consorcio',
  )
}}

WITH 
    linha_consorcio AS (
        SELECT
            data,
            timestamp_captura,
            SAFE_CAST(CD_CONSORCIO AS STRING) AS cd_consorcio,
            SAFE_CAST(CD_LINHA AS STRING) AS cd_linha,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS datetime_inclusao,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%d', SAFE_CAST(JSON_VALUE(content, '$.DT_INICIO_VALIDADE') AS STRING)), "America/Sao_Paulo") AS datetime_inicio_validade,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%d', SAFE_CAST(JSON_VALUE(content, '$.DT_FIM_VALIDADE') AS STRING)), "America/Sao_Paulo") AS datetime_fim_validade
        FROM
            {{ source("br_rj_riodejaneiro_bilhetagem_staging", "linha_consorcio") }}
    ),
    linha_consorcio_rn AS (
        SELECT
            *,
            CASE
              WHEN datetime_fim_validade IS NULL THEN ROW_NUMBER() OVER (PARTITION BY cd_linha ORDER BY timestamp_captura DESC, datetime_inicio_validade DESC)
              ELSE ROW_NUMBER() OVER (PARTITION BY cd_consorcio, cd_linha ORDER BY timestamp_captura DESC)
            END AS rn
        FROM
            linha_consorcio
    )
SELECT
  * EXCEPT(rn)
FROM
  linha_consorcio_rn
WHERE
  rn = 1