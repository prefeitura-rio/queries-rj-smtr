{{
  config(
    alias='consorcio',
  )
}}

WITH 
    consorcio AS (
        SELECT
            data,
            SAFE_CAST(CD_CONSORCIO AS STRING) AS cd_consorcio,
            timestamp_captura,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS datetime_inclusao,
            SAFE_CAST(JSON_VALUE(content, '$.NM_CONSORCIO') AS STRING) AS nm_consorcio
        FROM
            {{ source("br_rj_riodejaneiro_bilhetagem_staging", "consorcio") }}
    ),
    consorcio_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cd_consorcio ORDER BY timestamp_captura DESC) AS rn
        FROM
            consorcio
    )
SELECT
  * EXCEPT(rn)
FROM
  consorcio_rn
WHERE
  rn = 1