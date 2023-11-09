{{
  config(
    alias='linha',
  )
}}

WITH 
    linha AS (
        SELECT
            data,
            timestamp_captura,
            SAFE_CAST(CD_LINHA AS STRING) AS cd_linha,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS datetime_inclusao,
            SAFE_CAST(JSON_VALUE(content, '$.CD_LINHA_OFICIAL') AS STRING) AS cd_linha_oficial,
            SAFE_CAST(JSON_VALUE(content, '$.CD_LOCAL_OPERACAO_LINHA') AS STRING) AS cd_local_operacao_linha,
            SAFE_CAST(JSON_VALUE(content, '$.CD_TIPO_CATEGORIA_LINHA') AS STRING) AS cd_tipo_categoria_linha,
            SAFE_CAST(JSON_VALUE(content, '$.CD_TIPO_LINHA') AS STRING) AS cd_tipo_linha,
            SAFE_CAST(JSON_VALUE(content, '$.CD_TIPO_MATRIZ_CALCULO_SUBSIDIO') AS STRING) AS cd_tipo_matriz_calculo_subsidio,
            SAFE_CAST(JSON_VALUE(content, '$.IN_SITUACAO_ATIVIDADE') AS STRING) AS in_situacao_atividade,
            SAFE_CAST(JSON_VALUE(content, '$.KM_LINHA') AS FLOAT64) AS km_linha,
            SAFE_CAST(JSON_VALUE(content, '$.LATITUDE_DESTINO') AS STRING) AS latitude_destino,
            SAFE_CAST(JSON_VALUE(content, '$.LATITUDE_ORIGEM') AS STRING) AS latitude_origem,
            SAFE_CAST(JSON_VALUE(content, '$.LONGITUDE_DESTINO') AS STRING) AS longitude_destino,
            SAFE_CAST(JSON_VALUE(content, '$.LONGITUDE_ORIGEM') AS STRING) AS longitude_origem,
            SAFE_CAST(JSON_VALUE(content, '$.NM_LINHA') AS STRING) AS nm_linha,
            SAFE_CAST(JSON_VALUE(content, '$.NR_LINHA') AS STRING) AS nr_linha,
            SAFE_CAST(JSON_VALUE(content, '$.QUANTIDADE_SECAO') AS STRING) AS quantidade_secao
        FROM
            {{ source("br_rj_riodejaneiro_bilhetagem_staging", "linha") }}
    ),
    linha_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cd_linha ORDER BY timestamp_captura DESC) AS rn
        FROM
            linha
    )
SELECT
  * EXCEPT(rn)
FROM
  linha_rn
WHERE
  rn = 1