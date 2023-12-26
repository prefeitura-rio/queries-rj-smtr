{{
  config(
    alias='operadora_transporte',
  )
}}

WITH 
    operadora_transporte AS (
        SELECT
            data,
            SAFE_CAST(CD_OPERADORA_TRANSPORTE AS STRING) AS cd_operadora_transporte,
            timestamp_captura,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS datetime_inclusao,
            SAFE_CAST(JSON_VALUE(content, '$.CD_CLIENTE') AS STRING) AS cd_cliente,
            SAFE_CAST(JSON_VALUE(content, '$.CD_TIPO_CLIENTE') AS STRING) AS cd_tipo_cliente,
            SAFE_CAST(JSON_VALUE(content, '$.CD_TIPO_MODAL') AS STRING) AS cd_tipo_modal,
            SAFE_CAST(JSON_VALUE(content, '$.IN_SITUACAO_ATIVIDADE') AS STRING) AS in_situacao_atividade,
            SAFE_CAST(JSON_VALUE(content, '$.DS_TIPO_MODAL') AS STRING) AS ds_tipo_modal
        FROM
            {{ source("br_rj_riodejaneiro_bilhetagem_staging", "operadora_transporte") }}
    ),
    operadora_transporte_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cd_operadora_transporte ORDER BY timestamp_captura DESC) AS rn
        FROM
            operadora_transporte
    )
SELECT
  * EXCEPT(rn)
FROM
  operadora_transporte_rn
WHERE
  rn = 1