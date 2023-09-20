{{
  config(
    schema='br_rj_riodejaneiro_bilhetagem_staging',
    alias='grupo',
  )
}}

WITH 
    grupo_linha AS (
        SELECT
            data,
            timestamp_captura,
            DATETIME(PARSE_TIMESTAMP('%a, %d %b %Y %T GMT', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS dt_inclusao,
            SAFE_CAST(JSON_VALUE(content, '$.CD_GRUPO') AS STRING) AS cd_grupo,
            SAFE_CAST(JSON_VALUE(content, '$.CD_TIPO_GRUPO') AS STRING) AS cd_tipo_grupo,
            SAFE_CAST(JSON_VALUE(content, '$.DS_GRUPO') AS STRING) AS ds_grupo,
        FROM
            {{ var("bilhetagem_grupo_staging") }}
    ),
    grupo_linha_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY data, cd_grupo) AS rn
        FROM
            grupo_linha
        ORDER BY
            data
    )
SELECT
  * EXCEPT(rn)
FROM
  linha_rn
WHERE
  rn = 1