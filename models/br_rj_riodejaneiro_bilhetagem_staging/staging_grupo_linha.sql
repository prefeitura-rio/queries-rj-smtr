{{
  config(
    alias='grupo_linha',
  )
}}

WITH 
    grupo_linha AS (
        SELECT
            data,
            timestamp_captura,
            SAFE_CAST(CD_GRUPO AS STRING) AS cd_grupo,
            SAFE_CAST(CD_LINHA AS STRING) AS cd_linha,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS datetime_inclusao,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INICIO_VALIDADE') AS STRING)), "America/Sao_Paulo") AS datetime_inicio_validade,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_FIM_VALIDADE') AS STRING)), "America/Sao_Paulo") AS datetime_fim_validade
        FROM
            {{ source("br_rj_riodejaneiro_bilhetagem_staging", "grupo_linha") }}
    ),
    grupo_linha_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cd_grupo, cd_linha ORDER BY timestamp_captura DESC) AS rn
        FROM
            grupo_linha
    )
SELECT
  * EXCEPT(rn)
FROM
  grupo_linha_rn
WHERE
  rn = 1