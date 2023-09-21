{{
  config(
    schema='br_rj_riodejaneiro_bilhetagem_staging',
    alias='grupo',
  )
}}

WITH 
    grupo AS (
        SELECT
            data,
            timestamp_captura,
            SAFE_CAST(CD_GRUPO AS STRING) AS cd_grupo,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS datetime_inclusao,
            SAFE_CAST(JSON_VALUE(content, '$.CD_TIPO_GRUPO') AS STRING) AS cd_tipo_grupo,
            SAFE_CAST(JSON_VALUE(content, '$.DS_GRUPO') AS STRING) AS ds_grupo,
        FROM
            {{ var("bilhetagem_grupo_staging") }}
    ),
    grupo_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY data, cd_grupo) AS rn
        FROM
            grupo
        ORDER BY
            data
    )
SELECT
  * EXCEPT(rn)
FROM
  grupo_rn
WHERE
  rn = 1
ORDER BY
  datetime_inclusao